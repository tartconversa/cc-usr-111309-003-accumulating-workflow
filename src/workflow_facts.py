"""累积快照事实层：将合成样品流程事件回放为可追溯尝试与阶段驻留。

只做事实建模：版本裁决、状态机回放、非法转换隔离、累积快照、
每attempt测验事实、阶段驻留区间与区间分解查询。不运行任何实验作业，
不预测通过率，不修改原始输入。

事实语义
--------
* 事件按 (event_seconds, 配置类型优先级, event_id) 排序后逐case回放。
* 同 event_id 最高 revision 胜出；withdrawn=true 退出回放但保留证据；
  同 event_id 同 revision 内容冲突直接拒绝（抛 RevisionConflictError）。
* 每次构建都从裁决后的事件全集重新回放，不用最新事件类型覆盖状态，
  因此版本更正可能改变既往合法性，更正后重建即可解除阻塞。
* 非法转换：事件进入隔离区，case 在该位置阻塞，后续事件挂起不跨过；
  快照只宣称到阻塞位置为止的事实。
* 截止 as_of：只用 event_seconds <= as_of 的事件；未结束区间右端取
  as_of 并标 open，open 驻留与已完成耗时分列，绝不混加。
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from dataclasses import dataclass, field
from pathlib import Path

BUSINESS_TYPES = (
    "received",
    "test_started",
    "test_passed",
    "test_failed",
    "rework_requested",
    "cancelled",
)

# 阶段（驻留状态）。pre 是收到 received 之前的虚拟态，不产生驻留区间。
PHASE_PRE = "pre"
PHASE_AWAITING_TEST = "awaiting_test"
PHASE_RUNNING = "running"
PHASE_AWAITING_REWORK = "awaiting_rework"
PHASE_REWORK = "rework"
PHASE_PASSED = "passed"
PHASE_CANCELLED = "cancelled"
TERMINAL_PHASES = (PHASE_PASSED, PHASE_CANCELLED)

# 阶段 -> 快照汇总类别：等待(前测等待+失败等待返工) / 运行 / 返工。
PHASE_ROLLUP = {
    PHASE_AWAITING_TEST: "wait",
    PHASE_RUNNING: "run",
    PHASE_AWAITING_REWORK: "wait",
    PHASE_REWORK: "rework",
}

# 合法转换表：运行中只能 pass/fail 结束尝试（cancelled 终止整个流程，
# 连带关闭未结尝试）；失败后必须 rework_requested 才能再次 started；
# 通过后为终态不得再开尝试；cancelled 终止尚未终止的流程。
LEGAL_NEXT = {
    PHASE_PRE: {"received"},
    PHASE_AWAITING_TEST: {"test_started", "cancelled"},
    PHASE_RUNNING: {"test_passed", "test_failed", "cancelled"},
    PHASE_AWAITING_REWORK: {"rework_requested", "cancelled"},
    PHASE_REWORK: {"test_started", "cancelled"},
    PHASE_PASSED: set(),
    PHASE_CANCELLED: set(),
}

SUPPORTED_STATE_DURATIONS = "half_open_between_events"
SUPPORTED_ILLEGAL_TRANSITION = "block_case_preserve_evidence"


class InputContractError(ValueError):
    """输入记录或配置违反契约。"""


class RevisionConflictError(ValueError):
    """同 event_id 同 revision 内容冲突，拒绝裁决。"""


class FactIntegrityError(RuntimeError):
    """事实完整性被破坏（如负时长），拒绝继续。"""


# ---------------------------------------------------------------------------
# 配置与事件契约
# ---------------------------------------------------------------------------


def load_config(path):
    config = json.loads(Path(path).read_text(encoding="utf-8"))
    priority = config.get("same_time_priority")
    if not isinstance(priority, list) or sorted(priority) != sorted(BUSINESS_TYPES):
        raise InputContractError("same_time_priority 必须是六种业务类型的全排列")
    if config.get("state_durations") != SUPPORTED_STATE_DURATIONS:
        raise InputContractError("仅支持 state_durations=half_open_between_events")
    if config.get("illegal_transition") != SUPPORTED_ILLEGAL_TRANSITION:
        raise InputContractError("仅支持 illegal_transition=block_case_preserve_evidence")
    as_of = config.get("as_of_seconds", [])
    if not isinstance(as_of, list) or any(not _is_seconds(v) or v < 0 for v in as_of):
        raise InputContractError("as_of_seconds 必须是非负秒数列表")
    return {
        "same_time_priority": list(priority),
        "as_of_seconds": list(as_of),
    }


def _is_seconds(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def validate_event(raw):
    """校验并规范化一条事件记录；违反契约抛 InputContractError。"""
    if not isinstance(raw, dict):
        raise InputContractError(f"事件必须是对象: {raw!r}")
    missing = [
        key
        for key in ("case_id", "event_id", "event_seconds", "type", "revision", "withdrawn")
        if key not in raw
    ]
    if missing:
        raise InputContractError(f"事件缺少字段 {missing}: {raw!r}")
    event = {
        "case_id": raw["case_id"],
        "event_id": raw["event_id"],
        "event_seconds": raw["event_seconds"],
        "type": raw["type"],
        "revision": raw["revision"],
        "withdrawn": raw["withdrawn"],
    }
    if not isinstance(event["case_id"], str) or not event["case_id"]:
        raise InputContractError(f"case_id 必须是非空字符串: {raw!r}")
    if not isinstance(event["event_id"], str) or not event["event_id"]:
        raise InputContractError(f"event_id 必须是非空字符串: {raw!r}")
    if not _is_seconds(event["event_seconds"]):
        raise InputContractError(f"event_seconds 必须是数值秒: {raw!r}")
    if event["event_seconds"] < 0:
        raise InputContractError(f"负时长拒绝：event_seconds<0: {raw!r}")
    if event["type"] not in BUSINESS_TYPES:
        raise InputContractError(f"未知业务类型: {raw!r}")
    if not isinstance(event["revision"], int) or isinstance(event["revision"], bool) or event["revision"] < 1:
        raise InputContractError(f"revision 必须是 >=1 的整数: {raw!r}")
    if not isinstance(event["withdrawn"], bool):
        raise InputContractError(f"withdrawn 必须是布尔值: {raw!r}")
    return event


def _content_fingerprint(event):
    """同 event_id 平级比较用的内容指纹（不含 revision 本身）。"""
    return (
        event["case_id"],
        event["event_seconds"],
        event["type"],
        event["withdrawn"],
    )


def resolve_events(raw_events):
    """版本裁决：同 event_id 最高 revision 替代旧版，withdrawn 退出回放。

    返回 (replayable, withdrawn, duplicates_dropped)。
    同 event_id 同 revision 内容冲突 -> RevisionConflictError（拒绝）。
    内容完全相同的重复投递视为幂等重发，去重不计冲突。
    """
    by_id: dict[str, dict[int, dict]] = {}
    duplicates = 0
    for raw in raw_events:
        event = validate_event(raw)
        revisions = by_id.setdefault(event["event_id"], {})
        known = revisions.get(event["revision"])
        if known is not None:
            if _content_fingerprint(known) != _content_fingerprint(event):
                raise RevisionConflictError(
                    f"event_id={event['event_id']} revision={event['revision']} 平级内容冲突，拒绝"
                )
            duplicates += 1
            continue
        revisions[event["revision"]] = event
    replayable, withdrawn = [], []
    for revisions in by_id.values():
        winner = revisions[max(revisions)]
        (withdrawn if winner["withdrawn"] else replayable).append(winner)
    return replayable, withdrawn, duplicates


def order_events(events, priority):
    """同刻按配置类型优先级再 event_id 排序。"""
    rank = {name: index for index, name in enumerate(priority)}
    return sorted(
        events,
        key=lambda e: (e["event_seconds"], rank[e["type"]], e["event_id"]),
    )


def input_version_of(replayable, withdrawn, priority):
    """输入版本指纹：裁决后全集（含退出回放的撤回事件）+ 排序配置。"""
    canonical = json.dumps(
        {
            "replayable": sorted(
                replayable, key=lambda e: (e["event_id"], e["revision"])
            ),
            "withdrawn": sorted(
                withdrawn, key=lambda e: (e["event_id"], e["revision"])
            ),
            "priority": list(priority),
        },
        sort_keys=True,
        ensure_ascii=False,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]


# ---------------------------------------------------------------------------
# 回放状态机与局部 case 索引
# ---------------------------------------------------------------------------


@dataclass
class Attempt:
    number: int
    started_at: float
    start_event: str
    ended_at: float | None = None
    outcome: str | None = None  # passed / failed / cancelled
    end_event: str | None = None

    def to_dict(self):
        return dict(self.__dict__)

    @classmethod
    def from_dict(cls, data):
        return cls(**data)


@dataclass
class Interval:
    """半开驻留区间 [start, end)；end=None 表示尚未结束。"""

    phase: str
    start: float
    attempt: int | None
    open_event: str
    end: float | None = None
    close_event: str | None = None

    def to_dict(self):
        return dict(self.__dict__)

    @classmethod
    def from_dict(cls, data):
        return cls(**data)


@dataclass
class CaseState:
    case_id: str
    phase: str = PHASE_PRE
    phase_since: float | None = None
    received_at: float | None = None
    passed_at: float | None = None
    cancelled_at: float | None = None
    starts: list = field(default_factory=list)
    attempts: list = field(default_factory=list)
    intervals: list = field(default_factory=list)
    blocked: bool = False
    block_event: dict | None = None
    quarantined: list = field(default_factory=list)
    held: list = field(default_factory=list)

    def to_dict(self):
        return {
            "case_id": self.case_id,
            "phase": self.phase,
            "phase_since": self.phase_since,
            "received_at": self.received_at,
            "passed_at": self.passed_at,
            "cancelled_at": self.cancelled_at,
            "starts": list(self.starts),
            "attempts": [a.to_dict() for a in self.attempts],
            "intervals": [i.to_dict() for i in self.intervals],
            "blocked": self.blocked,
            "block_event": self.block_event,
            "quarantined": list(self.quarantined),
            "held": list(self.held),
        }

    @classmethod
    def from_dict(cls, data):
        state = cls(case_id=data["case_id"])
        state.phase = data["phase"]
        state.phase_since = data["phase_since"]
        state.received_at = data["received_at"]
        state.passed_at = data["passed_at"]
        state.cancelled_at = data["cancelled_at"]
        state.starts = list(data["starts"])
        state.attempts = [Attempt.from_dict(a) for a in data["attempts"]]
        state.intervals = [Interval.from_dict(i) for i in data["intervals"]]
        state.blocked = data["blocked"]
        state.block_event = data["block_event"]
        state.quarantined = list(data["quarantined"])
        state.held = list(data["held"])
        return state


def _open_interval(case, phase, at, attempt, event_id):
    case.phase = phase
    case.phase_since = at
    case.intervals.append(
        Interval(phase=phase, start=at, attempt=attempt, open_event=event_id)
    )


def _close_current_interval(case, event):
    if case.phase == PHASE_PRE:
        return
    interval = case.intervals[-1]
    if interval.end is not None:
        raise FactIntegrityError(f"case={case.case_id} 区间重复关闭")
    if event["event_seconds"] < interval.start:
        raise FactIntegrityError(
            f"负时长拒绝：case={case.case_id} 区间 [{interval.start}, {event['event_seconds']})"
        )
    interval.end = event["event_seconds"]
    interval.close_event = event["event_id"]


def _close_open_attempt(case, event, outcome):
    attempt = case.attempts[-1]
    if attempt.ended_at is not None:
        raise FactIntegrityError(f"case={case.case_id} attempt 重复结束")
    if event["event_seconds"] < attempt.started_at:
        raise FactIntegrityError(
            f"负时长拒绝：case={case.case_id} attempt {attempt.number} "
            f"[{attempt.started_at}, {event['event_seconds']})"
        )
    attempt.ended_at = event["event_seconds"]
    attempt.outcome = outcome
    attempt.end_event = event["event_id"]


def apply_event(case, event):
    """应用一条已排序事件到 case 状态机；非法转换隔离并阻塞，后续挂起。"""
    if case.blocked:
        case.held.append(
            {
                "event_id": event["event_id"],
                "type": event["type"],
                "event_seconds": event["event_seconds"],
                "revision": event["revision"],
                "reason": "held_after_block",
                "blocked_by": case.block_event["event_id"],
            }
        )
        return
    event_type = event["type"]
    if event_type not in LEGAL_NEXT[case.phase]:
        case.blocked = True
        case.block_event = dict(event)
        case.quarantined.append(
            {
                "event_id": event["event_id"],
                "type": event_type,
                "event_seconds": event["event_seconds"],
                "revision": event["revision"],
                "reason": "illegal_transition",
                "phase_at_block": case.phase,
                "expected": sorted(LEGAL_NEXT[case.phase]),
            }
        )
        return

    at = event["event_seconds"]
    _close_current_interval(case, event)
    if event_type == "received":
        case.received_at = at
        _open_interval(case, PHASE_AWAITING_TEST, at, None, event["event_id"])
    elif event_type == "test_started":
        attempt = Attempt(
            number=len(case.attempts) + 1, started_at=at, start_event=event["event_id"]
        )
        case.attempts.append(attempt)
        case.starts.append(at)
        _open_interval(case, PHASE_RUNNING, at, attempt.number, event["event_id"])
    elif event_type == "test_passed":
        _close_open_attempt(case, event, "passed")
        case.passed_at = at
        case.phase = PHASE_PASSED
        case.phase_since = at
    elif event_type == "test_failed":
        _close_open_attempt(case, event, "failed")
        _open_interval(case, PHASE_AWAITING_REWORK, at, case.attempts[-1].number, event["event_id"])
    elif event_type == "rework_requested":
        _open_interval(case, PHASE_REWORK, at, case.attempts[-1].number, event["event_id"])
    elif event_type == "cancelled":
        if case.attempts and case.attempts[-1].ended_at is None:
            _close_open_attempt(case, event, "cancelled")
        case.cancelled_at = at
        case.phase = PHASE_CANCELLED
        case.phase_since = at


class CaseIndex:
    """局部 case 索引：case_id -> 状态机。整体可序列化，支撑进程恢复。"""

    def __init__(self, priority):
        self.priority = list(priority)
        self.cases: dict[str, CaseState] = {}
        self.applied = 0

    def apply(self, event):
        case = self.cases.get(event["case_id"])
        if case is None:
            case = self.cases[event["case_id"]] = CaseState(event["case_id"])
        apply_event(case, event)
        self.applied += 1

    def to_dict(self):
        return {
            "priority": self.priority,
            "applied": self.applied,
            "cases": {cid: c.to_dict() for cid, c in sorted(self.cases.items())},
        }

    @classmethod
    def from_dict(cls, data):
        index = cls(data["priority"])
        index.applied = data["applied"]
        index.cases = {
            cid: CaseState.from_dict(c) for cid, c in data["cases"].items()
        }
        return index

    def checkpoint(self, path):
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(
            json.dumps(self.to_dict(), ensure_ascii=False, sort_keys=True, indent=2),
            encoding="utf-8",
        )

    @classmethod
    def load_checkpoint(cls, path):
        return cls.from_dict(json.loads(Path(path).read_text(encoding="utf-8")))


# ---------------------------------------------------------------------------
# 事实构建：累积快照 / 每attempt测验事实 / 阶段驻留区间
# ---------------------------------------------------------------------------


def interval_rows(case, as_of):
    """每段状态驻留区间一行；未结束区间右端取 as_of 并标 open。"""
    rows = []
    for interval in case.intervals:
        if interval.end is None:
            end, open_flag, close_event = as_of, True, None
        else:
            end, open_flag, close_event = interval.end, False, interval.close_event
        seconds = end - interval.start
        if seconds < 0:
            raise FactIntegrityError(
                f"负时长拒绝：case={case.case_id} 区间 [{interval.start}, {end})"
            )
        rows.append(
            {
                "case_id": case.case_id,
                "phase": interval.phase,
                "rollup": PHASE_ROLLUP[interval.phase],
                "attempt": interval.attempt,
                "start_seconds": interval.start,
                "end_seconds": end,
                "seconds": seconds,
                "open": open_flag,
                "open_event": interval.open_event,
                "close_event": close_event,
            }
        )
    return rows


def attempt_rows(case, as_of):
    """每 attempt 的测验事实；未结尝试只记已驻留，不记完成耗时。"""
    rows = []
    for attempt in case.attempts:
        if attempt.ended_at is None:
            rows.append(
                {
                    "case_id": case.case_id,
                    "attempt": attempt.number,
                    "started_at": attempt.started_at,
                    "ended_at": None,
                    "outcome": None,
                    "open": True,
                    "run_seconds": None,
                    "dwelled_seconds": as_of - attempt.started_at,
                    "start_event": attempt.start_event,
                    "end_event": None,
                }
            )
        else:
            rows.append(
                {
                    "case_id": case.case_id,
                    "attempt": attempt.number,
                    "started_at": attempt.started_at,
                    "ended_at": attempt.ended_at,
                    "outcome": attempt.outcome,
                    "open": False,
                    "run_seconds": attempt.ended_at - attempt.started_at,
                    "dwelled_seconds": None,
                    "start_event": attempt.start_event,
                    "end_event": attempt.end_event,
                }
            )
    return rows


def snapshot_row(case, as_of, input_version):
    """一行每 case 的累积快照。open 驻留与已完成耗时分列。"""
    intervals = interval_rows(case, as_of)
    rollups = {}
    for name in ("wait", "run", "rework"):
        rollups[f"{name}_seconds_completed"] = sum(
            r["seconds"] for r in intervals if r["rollup"] == name and not r["open"]
        )
        rollups[f"{name}_seconds_open"] = sum(
            r["seconds"] for r in intervals if r["rollup"] == name and r["open"]
        )
    completed_total = sum(v for k, v in rollups.items() if k.endswith("_completed"))
    open_total = sum(v for k, v in rollups.items() if k.endswith("_open"))

    terminal = case.phase in TERMINAL_PHASES
    if case.received_at is None:
        flow_seconds = None
    elif terminal:
        flow_seconds = case.phase_since - case.received_at
    else:
        flow_seconds = as_of - case.received_at
    conserved = (
        flow_seconds is not None
        and abs((completed_total + open_total) - flow_seconds) < 1e-9
    )

    open_interval = next((r for r in intervals if r["open"]), None)
    return {
        "case_id": case.case_id,
        "as_of_seconds": as_of,
        "input_version": input_version,
        "phase": case.phase,
        "terminal": terminal,
        "blocked": case.blocked,
        "block_event": case.block_event["event_id"] if case.block_event else None,
        "received_at": case.received_at,
        "first_test_started_at": case.starts[0] if case.starts else None,
        "last_test_started_at": case.starts[-1] if case.starts else None,
        "passed_at": case.passed_at,
        "cancelled_at": case.cancelled_at,
        "attempts": len(case.attempts),
        "attempts_open": sum(1 for a in case.attempts if a.ended_at is None),
        **rollups,
        "non_run_seconds_completed": rollups["wait_seconds_completed"]
        + rollups["rework_seconds_completed"],
        "non_run_seconds_open": rollups["wait_seconds_open"]
        + rollups["rework_seconds_open"],
        "open_phase": open_interval["phase"] if open_interval else None,
        "open_phase_since": open_interval["start_seconds"] if open_interval else None,
        "open_phase_seconds": open_interval["seconds"] if open_interval else 0,
        "flow_seconds": flow_seconds,
        "conserved": conserved,
        "quarantined_events": len(case.quarantined),
        "held_events": len(case.held),
    }


def quarantine_rows(case, as_of):
    rows = []
    for entry in case.quarantined:
        rows.append({"case_id": case.case_id, "as_of_seconds": as_of, **entry})
    for entry in case.held:
        rows.append({"case_id": case.case_id, "as_of_seconds": as_of, **entry})
    return rows


class FactSet:
    """一次构建（一个 as_of）的全部事实。"""

    def __init__(self, index, as_of, input_version, ordered_events, withdrawn):
        self.index = index
        self.as_of = as_of
        self.input_version = input_version
        self.ordered_events = ordered_events
        self.withdrawn = withdrawn

    def snapshots(self):
        return [
            snapshot_row(case, self.as_of, self.input_version)
            for case in sorted(self.index.cases.values(), key=lambda c: c.case_id)
        ]

    def attempts(self):
        rows = []
        for case in sorted(self.index.cases.values(), key=lambda c: c.case_id):
            rows.extend(
                {"as_of_seconds": self.as_of, **row} for row in attempt_rows(case, self.as_of)
            )
        return rows

    def intervals(self):
        rows = []
        for case in sorted(self.index.cases.values(), key=lambda c: c.case_id):
            rows.extend(
                {"as_of_seconds": self.as_of, **row} for row in interval_rows(case, self.as_of)
            )
        return rows

    def quarantine(self):
        rows = []
        for case in sorted(self.index.cases.values(), key=lambda c: c.case_id):
            rows.extend(quarantine_rows(case, self.as_of))
        return rows


def build_facts(raw_events, config, as_of):
    """从裁决后事件全集重新回放构建 as_of 时点事实（不增量覆盖状态）。"""
    if not _is_seconds(as_of) or as_of < 0:
        raise InputContractError(f"as_of 必须是非负秒数: {as_of!r}")
    replayable, withdrawn, _ = resolve_events(raw_events)
    version = input_version_of(replayable, withdrawn, config["same_time_priority"])
    eligible = [e for e in replayable if e["event_seconds"] <= as_of]
    ordered = order_events(eligible, config["same_time_priority"])
    index = CaseIndex(config["same_time_priority"])
    for event in ordered:
        index.apply(event)
    return FactSet(index, as_of, version, ordered, withdrawn)


def decompose_intervals(interval_rows_for_case, start, end):
    """区间分解查询：把 [start, end) 窗口分解为阶段驻留片段与汇总。"""
    if end < start:
        raise InputContractError(f"窗口右端小于左端: [{start}, {end})")
    segments = []
    for row in interval_rows_for_case:
        lo = max(start, row["start_seconds"])
        hi = min(end, row["end_seconds"])
        if hi < lo:
            continue
        segments.append(
            {
                "phase": row["phase"],
                "rollup": row["rollup"],
                "attempt": row["attempt"],
                "start_seconds": lo,
                "end_seconds": hi,
                "seconds": hi - lo,
                "open": row["open"],
            }
        )
    totals = {}
    for segment in segments:
        key = (segment["rollup"], segment["open"])
        totals[key] = totals.get(key, 0) + segment["seconds"]
    return {
        "window": [start, end],
        "segments": segments,
        "totals": {
            f"{rollup}_{'open' if open_flag else 'completed'}": seconds
            for (rollup, open_flag), seconds in sorted(totals.items())
        },
    }


# ---------------------------------------------------------------------------
# 输出与命令行
# ---------------------------------------------------------------------------


def _write_jsonl(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as stream:
        for row in rows:
            stream.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")


def write_factsets(factsets, out_dir):
    """写出事实层：累积快照 / 每attempt事实 / 驻留区间 / 隔离区 / 运行元数据。"""
    out = Path(out_dir)
    snapshots, attempts, intervals, quarantine = [], [], [], []
    for factset in factsets:
        snapshots.extend(factset.snapshots())
        attempts.extend(factset.attempts())
        intervals.extend(factset.intervals())
        quarantine.extend(factset.quarantine())
    _write_jsonl(out / "case_snapshot.jsonl", snapshots)
    _write_jsonl(out / "attempt_facts.jsonl", attempts)
    _write_jsonl(out / "state_intervals.jsonl", intervals)
    _write_jsonl(out / "quarantine.jsonl", quarantine)
    metadata = {
        "input_version": factsets[0].input_version if factsets else None,
        "as_of_seconds": [f.as_of for f in factsets],
        "applied_events": {str(f.as_of): f.index.applied for f in factsets},
        "withdrawn_events": {
            str(f.as_of): [e["event_id"] for e in f.withdrawn] for f in factsets
        },
    }
    (out / "run_metadata.json").write_text(
        json.dumps(metadata, ensure_ascii=False, sort_keys=True, indent=2),
        encoding="utf-8",
    )
    return metadata


def _load_events(path):
    from src.workflow_reader import load

    return load(path)


def _cmd_build(args):
    config = load_config(args.config)
    raw_events = _load_events(args.events)
    as_of_list = args.as_of if args.as_of else config["as_of_seconds"]
    if not as_of_list:
        raise InputContractError("未指定 as_of（命令行或配置 as_of_seconds）")
    factsets = [build_facts(raw_events, config, as_of) for as_of in as_of_list]
    metadata = write_factsets(factsets, args.out)
    print(json.dumps(metadata, ensure_ascii=False, sort_keys=True))


def _cmd_replay(args):
    """事件回放（支撑进程恢复）：可限步、可写检查点、可从检查点恢复。"""
    config = load_config(args.config)
    raw_events = _load_events(args.events)
    replayable, withdrawn, _ = resolve_events(raw_events)
    version = input_version_of(replayable, withdrawn, config["same_time_priority"])
    eligible = [e for e in replayable if e["event_seconds"] <= args.as_of]
    ordered = order_events(eligible, config["same_time_priority"])

    start = 0
    if args.resume:
        index = CaseIndex.load_checkpoint(args.checkpoint)
        start = index.applied
        if start > len(ordered):
            raise InputContractError("检查点游标超过当前事件流，输入版本可能已变化")
    else:
        index = CaseIndex(config["same_time_priority"])

    stop = len(ordered) if args.stop_after is None else min(args.stop_after, len(ordered))
    for position in range(start, stop):
        index.apply(ordered[position])
    if args.checkpoint:
        index.checkpoint(args.checkpoint)

    factset = FactSet(index, args.as_of, version, ordered[:stop], withdrawn)
    metadata = write_factsets([factset], args.out)
    metadata["resumed_from"] = start
    metadata["applied"] = index.applied
    print(json.dumps(metadata, ensure_ascii=False, sort_keys=True))


def _cmd_decompose(args):
    """区间分解查询：指定 case 在窗口内的阶段驻留分解。"""
    config = load_config(args.config)
    raw_events = _load_events(args.events)
    factset = build_facts(raw_events, config, args.as_of)
    case = factset.index.cases.get(args.case)
    if case is None:
        raise InputContractError(f"未知 case: {args.case}")
    rows = interval_rows(case, args.as_of)
    start = args.start if args.start is not None else (case.received_at or 0)
    end = args.end if args.end is not None else args.as_of
    result = decompose_intervals(rows, start, end)
    result["case_id"] = args.case
    result["as_of_seconds"] = args.as_of
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2))


def main(argv=None):
    parser = argparse.ArgumentParser(description="合成样品流程的累积快照事实层")
    sub = parser.add_subparsers(dest="command", required=True)

    build = sub.add_parser("build", help="按配置构建全部 as_of 的事实层")
    build.add_argument("--events", required=True)
    build.add_argument("--config", required=True)
    build.add_argument("--out", required=True)
    build.add_argument("--as-of", type=float, action="append", default=None)
    build.set_defaults(func=_cmd_build)

    replay = sub.add_parser("replay", help="事件回放，支持检查点与恢复")
    replay.add_argument("--events", required=True)
    replay.add_argument("--config", required=True)
    replay.add_argument("--out", required=True)
    replay.add_argument("--as-of", type=float, required=True)
    replay.add_argument("--checkpoint", default=None)
    replay.add_argument("--stop-after", type=int, default=None)
    replay.add_argument("--resume", action="store_true")
    replay.set_defaults(func=_cmd_replay)

    decompose = sub.add_parser("decompose", help="区间分解查询")
    decompose.add_argument("--events", required=True)
    decompose.add_argument("--config", required=True)
    decompose.add_argument("--case", required=True)
    decompose.add_argument("--as-of", type=float, required=True)
    decompose.add_argument("--start", type=float, default=None)
    decompose.add_argument("--end", type=float, default=None)
    decompose.set_defaults(func=_cmd_decompose)

    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
