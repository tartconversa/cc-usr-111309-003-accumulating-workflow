"""场景演示：撤回与补正、同刻排序、取消、进程恢复、区间分解查询。

全部使用合成事件，输出写到 .workspace-output/demo，不触碰原始输入。
运行：python -m src.workflow_demo
"""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path

from src.workflow_facts import (
    build_facts,
    decompose_intervals,
    interval_rows,
    load_config,
    resolve_events,
)

ROOT = Path(__file__).resolve().parents[1]
FIXTURE_EVENTS = ROOT / "fixtures" / "workflow.jsonl"
FIXTURE_CONFIG = ROOT / "fixtures" / "workflow.json"
DEMO_OUT = ROOT / ".workspace-output" / "demo"

CONFIG = load_config(FIXTURE_CONFIG)


def _base_events():
    """与 fixtures/workflow.jsonl 一致的合成主流程（秒）。"""
    return [
        {"case_id": "s1", "event_id": "e0", "event_seconds": 0, "type": "received", "revision": 1, "withdrawn": False},
        {"case_id": "s1", "event_id": "e1", "event_seconds": 2, "type": "test_started", "revision": 1, "withdrawn": False},
        {"case_id": "s1", "event_id": "e2", "event_seconds": 5, "type": "test_failed", "revision": 1, "withdrawn": False},
        {"case_id": "s1", "event_id": "e3", "event_seconds": 6, "type": "rework_requested", "revision": 1, "withdrawn": False},
        {"case_id": "s1", "event_id": "e4", "event_seconds": 8, "type": "test_started", "revision": 1, "withdrawn": False},
        {"case_id": "s1", "event_id": "e5", "event_seconds": 12, "type": "test_passed", "revision": 1, "withdrawn": False},
    ]


def _snapshot(factset, case_id="s1"):
    return next(row for row in factset.snapshots() if row["case_id"] == case_id)


def scenario_baseline():
    """守恒示例：两次运行3+4=7，非运行2+1+2=5，流程12秒守恒。"""
    factset = build_facts(_base_events(), CONFIG, as_of=12)
    snap = _snapshot(factset)
    return {
        "snapshot": snap,
        "attempts": factset.attempts(),
        "check": {
            "run_completed_7": snap["run_seconds_completed"] == 7,
            "non_run_completed_5": snap["non_run_seconds_completed"] == 5,
            "flow_12_conserved": snap["flow_seconds"] == 12 and snap["conserved"],
        },
    }


def scenario_as_of_open_interval():
    """截止10秒：第二次运行仅驻留2秒且open，不得记成完成4秒。"""
    factset = build_facts(_base_events(), CONFIG, as_of=10)
    snap = _snapshot(factset)
    attempt2 = next(a for a in factset.attempts() if a["attempt"] == 2)
    return {
        "snapshot": snap,
        "attempt2": attempt2,
        "check": {
            "run_completed_3": snap["run_seconds_completed"] == 3,
            "run_open_2": snap["run_seconds_open"] == 2,
            "attempt2_open_dwelled_2": attempt2["open"]
            and attempt2["dwelled_seconds"] == 2
            and attempt2["run_seconds"] is None,
            "flow_10_conserved": snap["flow_seconds"] == 10 and snap["conserved"],
        },
    }


def scenario_withdraw_and_correct():
    """撤回失败判定 -> 后续事件失去合法前提，case 阻塞；补正后重建解除。"""
    withdrawn_fail = _base_events() + [
        {"case_id": "s1", "event_id": "e2", "event_seconds": 5, "type": "test_failed", "revision": 2, "withdrawn": True},
    ]
    blocked = build_facts(withdrawn_fail, CONFIG, as_of=12)
    blocked_snap = _snapshot(blocked)

    corrected = withdrawn_fail + [
        {"case_id": "s1", "event_id": "e2", "event_seconds": 5, "type": "test_failed", "revision": 3, "withdrawn": False},
    ]
    rebuilt = build_facts(corrected, CONFIG, as_of=12)
    rebuilt_snap = _snapshot(rebuilt)
    return {
        "blocked_snapshot": blocked_snap,
        "quarantine": blocked.quarantine(),
        "rebuilt_snapshot": rebuilt_snap,
        "check": {
            "blocked_at_e3": blocked_snap["blocked"] and blocked_snap["block_event"] == "e3",
            "held_not_applied": blocked_snap["held_events"] == 2
            and blocked_snap["passed_at"] is None,
            "rebuild_unblocks": not rebuilt_snap["blocked"]
            and rebuilt_snap["passed_at"] == 12
            and rebuilt_snap["run_seconds_completed"] == 7,
        },
    }


def scenario_same_timestamp():
    """同刻按配置类型优先级再 event_id 排序：fail 先于 rework，零长区间留痕。"""
    events = [
        {"case_id": "s2", "event_id": "a0", "event_seconds": 0, "type": "received", "revision": 1, "withdrawn": False},
        {"case_id": "s2", "event_id": "a1", "event_seconds": 2, "type": "test_started", "revision": 1, "withdrawn": False},
        {"case_id": "s2", "event_id": "a2", "event_seconds": 5, "type": "rework_requested", "revision": 1, "withdrawn": False},
        {"case_id": "s2", "event_id": "a3", "event_seconds": 5, "type": "test_failed", "revision": 1, "withdrawn": False},
        {"case_id": "s2", "event_id": "a4", "event_seconds": 9, "type": "test_started", "revision": 1, "withdrawn": False},
        {"case_id": "s2", "event_id": "a5", "event_seconds": 11, "type": "test_passed", "revision": 1, "withdrawn": False},
    ]
    factset = build_facts(events, CONFIG, as_of=11)
    ordered_types = [e["type"] for e in factset.ordered_events if e["event_seconds"] == 5]
    zero_interval = next(
        row
        for row in factset.intervals()
        if row["phase"] == "awaiting_rework" and row["seconds"] == 0
    )
    snap = _snapshot(factset, "s2")
    return {
        "same_second_order": ordered_types,
        "zero_length_interval": zero_interval,
        "snapshot": snap,
        "check": {
            "fail_before_rework": ordered_types == ["test_failed", "rework_requested"],
            "not_blocked": not snap["blocked"] and snap["passed_at"] == 11,
        },
    }


def scenario_cancel():
    """cancelled 终止尚未终止的流程：运行中取消连带关闭尝试；终态后取消非法。"""
    events = [
        {"case_id": "s3", "event_id": "c0", "event_seconds": 0, "type": "received", "revision": 1, "withdrawn": False},
        {"case_id": "s3", "event_id": "c1", "event_seconds": 2, "type": "test_started", "revision": 1, "withdrawn": False},
        {"case_id": "s3", "event_id": "c2", "event_seconds": 7, "type": "cancelled", "revision": 1, "withdrawn": False},
        {"case_id": "s4", "event_id": "d0", "event_seconds": 0, "type": "received", "revision": 1, "withdrawn": False},
        {"case_id": "s4", "event_id": "d1", "event_seconds": 3, "type": "cancelled", "revision": 1, "withdrawn": False},
        {"case_id": "s5", "event_id": "f0", "event_seconds": 0, "type": "received", "revision": 1, "withdrawn": False},
        {"case_id": "s5", "event_id": "f1", "event_seconds": 2, "type": "test_started", "revision": 1, "withdrawn": False},
        {"case_id": "s5", "event_id": "f2", "event_seconds": 6, "type": "test_passed", "revision": 1, "withdrawn": False},
        {"case_id": "s5", "event_id": "f3", "event_seconds": 9, "type": "cancelled", "revision": 1, "withdrawn": False},
    ]
    factset = build_facts(events, CONFIG, as_of=9)
    snaps = {row["case_id"]: row for row in factset.snapshots()}
    attempt_s3 = next(a for a in factset.attempts() if a["case_id"] == "s3")
    return {
        "snapshots": snaps,
        "attempt_s3": attempt_s3,
        "check": {
            "running_cancelled": snaps["s3"]["phase"] == "cancelled"
            and attempt_s3["outcome"] == "cancelled"
            and attempt_s3["run_seconds"] == 5,
            "awaiting_cancelled": snaps["s4"]["phase"] == "cancelled"
            and snaps["s4"]["attempts"] == 0,
            "cancel_after_terminal_blocked": snaps["s5"]["blocked"]
            and snaps["s5"]["block_event"] == "f3"
            and snaps["s5"]["passed_at"] == 6,
        },
    }


def scenario_process_recovery():
    """进程恢复：回放中途写检查点，模拟重启后续放，结果与一次性回放一致。"""
    with tempfile.TemporaryDirectory(dir=DEMO_OUT) as directory:
        work = Path(directory)
        events_path = work / "events.jsonl"
        events_path.write_text(
            "".join(json.dumps(e, ensure_ascii=False) + "\n" for e in _base_events()),
            encoding="utf-8",
        )
        checkpoint = work / "checkpoint.json"
        full_out, part_out, resumed_out = work / "full", work / "part", work / "resumed"

        def replay(*extra):
            subprocess.run(
                [
                    sys.executable, "-m", "src.workflow_facts", "replay",
                    "--events", str(events_path),
                    "--config", str(FIXTURE_CONFIG),
                    "--as-of", "12",
                    *extra,
                ],
                cwd=ROOT,
                check=True,
                capture_output=True,
                text=True,
            )

        replay("--out", str(full_out))
        replay("--out", str(part_out), "--stop-after", "2", "--checkpoint", str(checkpoint))
        replay("--out", str(resumed_out), "--checkpoint", str(checkpoint), "--resume")

        def read(name, out_dir):
            return (out_dir / name).read_text(encoding="utf-8")

        same = all(
            read(name, full_out) == read(name, resumed_out)
            for name in ("case_snapshot.jsonl", "attempt_facts.jsonl", "state_intervals.jsonl")
        )
        partial_snapshot = json.loads(read("case_snapshot.jsonl", part_out).strip())
        return {
            "partial_after_2_events": partial_snapshot,
            "check": {
                "resumed_equals_full": same,
                "partial_was_running": partial_snapshot["phase"] == "running"
                and partial_snapshot["attempts"] == 1,
            },
        }


def scenario_decompose():
    """区间分解查询：主流程在 [0,12) 与 [0,10) 窗口内的阶段分解。"""
    factset12 = build_facts(_base_events(), CONFIG, as_of=12)
    case = factset12.index.cases["s1"]
    full = decompose_intervals(interval_rows(case, 12), 0, 12)
    factset10 = build_facts(_base_events(), CONFIG, as_of=10)
    case10 = factset10.index.cases["s1"]
    windowed = decompose_intervals(interval_rows(case10, 10), 0, 10)
    return {
        "window_0_12": full,
        "window_0_10": windowed,
        "check": {
            "full_totals": full["totals"]
            == {"run_completed": 7, "rework_completed": 2, "wait_completed": 3},
            "windowed_open_run_2": windowed["totals"].get("run_open") == 2
            and windowed["totals"].get("run_completed") == 3,
        },
    }


SCENARIOS = [
    ("baseline_conservation", scenario_baseline),
    ("as_of_open_interval", scenario_as_of_open_interval),
    ("withdraw_and_correct", scenario_withdraw_and_correct),
    ("same_timestamp", scenario_same_timestamp),
    ("cancel", scenario_cancel),
    ("process_recovery", scenario_process_recovery),
    ("decompose_query", scenario_decompose),
]


def main():
    DEMO_OUT.mkdir(parents=True, exist_ok=True)
    report = {}
    all_ok = True
    for name, scenario in SCENARIOS:
        result = scenario()
        ok = all(result["check"].values())
        all_ok = all_ok and ok
        report[name] = {"ok": ok, **result}
        print(f"[{'OK' if ok else 'FAIL'}] {name}")
    (DEMO_OUT / "demo_report.json").write_text(
        json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2),
        encoding="utf-8",
    )
    print(f"报告写入 {DEMO_OUT / 'demo_report.json'}")
    if not all_ok:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
