import json
import tempfile
import unittest
from pathlib import Path

from src.workflow_facts import (
    CaseIndex,
    FactIntegrityError,
    InputContractError,
    RevisionConflictError,
    build_facts,
    decompose_intervals,
    interval_rows,
    load_config,
    order_events,
    resolve_events,
    validate_event,
)

ROOT = Path(__file__).resolve().parents[1]
CONFIG = load_config(ROOT / "fixtures" / "workflow.json")


def ev(case, event_id, seconds, type_, revision=1, withdrawn=False):
    return {
        "case_id": case,
        "event_id": event_id,
        "event_seconds": seconds,
        "type": type_,
        "revision": revision,
        "withdrawn": withdrawn,
    }


def base_flow():
    """received=0 start=2 fail=5 rework=6 start=8 pass=12。"""
    return [
        ev("s1", "e0", 0, "received"),
        ev("s1", "e1", 2, "test_started"),
        ev("s1", "e2", 5, "test_failed"),
        ev("s1", "e3", 6, "rework_requested"),
        ev("s1", "e4", 8, "test_started"),
        ev("s1", "e5", 12, "test_passed"),
    ]


def snapshot_of(factset, case_id="s1"):
    return next(row for row in factset.snapshots() if row["case_id"] == case_id)


class VersionResolutionTests(unittest.TestCase):
    def test_highest_revision_replaces(self):
        events = [
            ev("s1", "e0", 0, "received", revision=1),
            ev("s1", "e0", 3, "received", revision=2),
        ]
        replayable, _, _ = resolve_events(events)
        self.assertEqual(len(replayable), 1)
        self.assertEqual(replayable[0]["event_seconds"], 3)

    def test_withdrawn_excluded_but_kept_as_evidence(self):
        events = base_flow() + [ev("s1", "e2", 5, "test_failed", revision=2, withdrawn=True)]
        replayable, withdrawn, _ = resolve_events(events)
        self.assertEqual([e["event_id"] for e in withdrawn], ["e2"])
        self.assertNotIn("e2", [e["event_id"] for e in replayable])

    def test_same_revision_conflict_rejected(self):
        events = [
            ev("s1", "e0", 0, "received"),
            ev("s1", "e0", 1, "received"),
        ]
        with self.assertRaises(RevisionConflictError):
            resolve_events(events)

    def test_identical_duplicate_is_idempotent(self):
        events = base_flow() + [dict(base_flow()[0])]
        replayable, _, duplicates = resolve_events(events)
        self.assertEqual(duplicates, 1)
        self.assertEqual(len(replayable), 6)

    def test_correction_changes_legality_requires_full_replay(self):
        # rework 的 revision 2 把时刻从 6 提前到 3：落在运行中，变为非法。
        events = base_flow() + [ev("s1", "e3", 3, "rework_requested", revision=2)]
        factset = build_facts(events, CONFIG, as_of=12)
        snap = snapshot_of(factset)
        self.assertTrue(snap["blocked"])
        self.assertEqual(snap["block_event"], "e3")
        self.assertIsNone(snap["passed_at"])


class OrderingTests(unittest.TestCase):
    def test_same_second_type_priority_then_event_id(self):
        events = [
            ev("s1", "b2", 5, "rework_requested"),
            ev("s1", "b1", 5, "test_failed"),
            ev("s1", "a9", 5, "test_failed"),
        ]
        ordered = order_events(events, CONFIG["same_time_priority"])
        self.assertEqual([e["event_id"] for e in ordered], ["a9", "b1", "b2"])

    def test_same_second_fail_then_rework_is_legal(self):
        events = [
            ev("s2", "a0", 0, "received"),
            ev("s2", "a1", 2, "test_started"),
            ev("s2", "a2", 5, "rework_requested"),
            ev("s2", "a3", 5, "test_failed"),
            ev("s2", "a4", 9, "test_started"),
            ev("s2", "a5", 11, "test_passed"),
        ]
        factset = build_facts(events, CONFIG, as_of=11)
        snap = snapshot_of(factset, "s2")
        self.assertFalse(snap["blocked"])
        self.assertEqual(snap["passed_at"], 11)
        zero = [
            row
            for row in factset.intervals()
            if row["phase"] == "awaiting_rework" and row["seconds"] == 0
        ]
        self.assertEqual(len(zero), 1)


class ConservationTests(unittest.TestCase):
    def test_full_flow_conservation(self):
        snap = snapshot_of(build_facts(base_flow(), CONFIG, as_of=12))
        self.assertEqual(snap["run_seconds_completed"], 7)
        self.assertEqual(snap["wait_seconds_completed"], 3)
        self.assertEqual(snap["rework_seconds_completed"], 2)
        self.assertEqual(snap["non_run_seconds_completed"], 5)
        self.assertEqual(snap["flow_seconds"], 12)
        self.assertTrue(snap["conserved"])
        self.assertEqual(snap["attempts"], 2)
        self.assertEqual(snap["first_test_started_at"], 2)
        self.assertEqual(snap["last_test_started_at"], 8)
        self.assertEqual(snap["passed_at"], 12)

    def test_as_of_open_run_not_counted_completed(self):
        factset = build_facts(base_flow(), CONFIG, as_of=10)
        snap = snapshot_of(factset)
        self.assertEqual(snap["run_seconds_completed"], 3)
        self.assertEqual(snap["run_seconds_open"], 2)
        self.assertEqual(snap["open_phase"], "running")
        self.assertEqual(snap["flow_seconds"], 10)
        self.assertTrue(snap["conserved"])
        attempt2 = next(a for a in factset.attempts() if a["attempt"] == 2)
        self.assertTrue(attempt2["open"])
        self.assertEqual(attempt2["dwelled_seconds"], 2)
        self.assertIsNone(attempt2["run_seconds"])

    def test_events_after_as_of_excluded(self):
        factset = build_facts(base_flow(), CONFIG, as_of=4)
        snap = snapshot_of(factset)
        self.assertEqual(snap["attempts"], 1)
        self.assertEqual(snap["phase"], "running")
        self.assertEqual(snap["run_seconds_open"], 2)


class IllegalTransitionTests(unittest.TestCase):
    def test_duplicate_start_rejected_and_blocks(self):
        events = [
            ev("s1", "e0", 0, "received"),
            ev("s1", "e1", 2, "test_started"),
            ev("s1", "e2", 4, "test_started"),
            ev("s1", "e3", 6, "test_passed"),
        ]
        factset = build_facts(events, CONFIG, as_of=6)
        snap = snapshot_of(factset)
        self.assertTrue(snap["blocked"])
        self.assertEqual(snap["block_event"], "e2")
        self.assertEqual(snap["attempts"], 1)
        self.assertIsNone(snap["passed_at"])
        quarantine = factset.quarantine()
        self.assertEqual(quarantine[0]["reason"], "illegal_transition")
        self.assertEqual(quarantine[1]["reason"], "held_after_block")

    def test_rework_while_running_blocks(self):
        events = [
            ev("s1", "e0", 0, "received"),
            ev("s1", "e1", 2, "test_started"),
            ev("s1", "e2", 4, "rework_requested"),
        ]
        snap = snapshot_of(build_facts(events, CONFIG, as_of=4))
        self.assertTrue(snap["blocked"])
        self.assertEqual(snap["phase"], "running")

    def test_start_after_pass_rejected(self):
        events = base_flow() + [ev("s1", "e6", 14, "test_started")]
        factset = build_facts(events, CONFIG, as_of=14)
        snap = snapshot_of(factset)
        self.assertTrue(snap["blocked"])
        self.assertEqual(snap["block_event"], "e6")
        self.assertEqual(snap["attempts"], 2)

    def test_case_must_start_with_received(self):
        events = [ev("s1", "e0", 0, "test_started")]
        snap = snapshot_of(build_facts(events, CONFIG, as_of=0))
        self.assertTrue(snap["blocked"])
        self.assertEqual(snap["phase"], "pre")
        self.assertIsNone(snap["flow_seconds"])

    def test_blocked_case_does_not_claim_completion(self):
        events = [
            ev("s1", "e0", 0, "received"),
            ev("s1", "e1", 2, "test_started"),
            ev("s1", "e2", 5, "test_failed"),
            ev("s1", "e3", 6, "test_started"),  # 缺 rework_requested
            ev("s1", "e4", 9, "test_passed"),
        ]
        factset = build_facts(events, CONFIG, as_of=9)
        snap = snapshot_of(factset)
        self.assertTrue(snap["blocked"])
        self.assertIsNone(snap["passed_at"])
        self.assertEqual(snap["held_events"], 1)
        self.assertEqual(snap["phase"], "awaiting_rework")


class WithdrawCorrectTests(unittest.TestCase):
    def test_withdraw_fail_blocks_then_correction_unblocks(self):
        withdrawn = base_flow() + [ev("s1", "e2", 5, "test_failed", revision=2, withdrawn=True)]
        blocked = snapshot_of(build_facts(withdrawn, CONFIG, as_of=12))
        self.assertTrue(blocked["blocked"])
        self.assertEqual(blocked["block_event"], "e3")
        self.assertEqual(blocked["held_events"], 2)
        self.assertIsNone(blocked["passed_at"])

        corrected = withdrawn + [ev("s1", "e2", 5, "test_failed", revision=3)]
        rebuilt = snapshot_of(build_facts(corrected, CONFIG, as_of=12))
        self.assertFalse(rebuilt["blocked"])
        self.assertEqual(rebuilt["passed_at"], 12)
        self.assertEqual(rebuilt["run_seconds_completed"], 7)


class CancelTests(unittest.TestCase):
    def test_cancel_while_running_closes_attempt(self):
        events = [
            ev("s1", "e0", 0, "received"),
            ev("s1", "e1", 2, "test_started"),
            ev("s1", "e2", 7, "cancelled"),
        ]
        factset = build_facts(events, CONFIG, as_of=7)
        snap = snapshot_of(factset)
        self.assertEqual(snap["phase"], "cancelled")
        self.assertTrue(snap["terminal"])
        self.assertEqual(snap["cancelled_at"], 7)
        attempt = factset.attempts()[0]
        self.assertEqual(attempt["outcome"], "cancelled")
        self.assertEqual(attempt["run_seconds"], 5)

    def test_cancel_after_terminal_rejected(self):
        events = base_flow() + [ev("s1", "e6", 15, "cancelled")]
        snap = snapshot_of(build_facts(events, CONFIG, as_of=15))
        self.assertTrue(snap["blocked"])
        self.assertEqual(snap["passed_at"], 12)


class IntegrityTests(unittest.TestCase):
    def test_negative_event_seconds_rejected(self):
        with self.assertRaises(InputContractError):
            validate_event(ev("s1", "e0", -1, "received"))

    def test_negative_as_of_rejected(self):
        with self.assertRaises(InputContractError):
            build_facts(base_flow(), CONFIG, as_of=-1)

    def test_unknown_type_rejected(self):
        with self.assertRaises(InputContractError):
            validate_event(ev("s1", "e0", 0, "exploded"))

    def test_missing_field_rejected(self):
        with self.assertRaises(InputContractError):
            validate_event({"case_id": "s1"})

    def test_decompose_rejects_inverted_window(self):
        with self.assertRaises(InputContractError):
            decompose_intervals([], 5, 2)


class DecomposeTests(unittest.TestCase):
    def test_window_clips_intervals(self):
        factset = build_facts(base_flow(), CONFIG, as_of=12)
        case = factset.index.cases["s1"]
        result = decompose_intervals(interval_rows(case, 12), 3, 9)
        self.assertEqual(
            [(s["phase"], s["seconds"]) for s in result["segments"]],
            [("running", 2), ("awaiting_rework", 1), ("rework", 2), ("running", 1)],
        )
        self.assertEqual(result["totals"]["run_completed"], 3)

    def test_open_tail_marked(self):
        factset = build_facts(base_flow(), CONFIG, as_of=10)
        case = factset.index.cases["s1"]
        result = decompose_intervals(interval_rows(case, 10), 0, 10)
        self.assertEqual(result["totals"]["run_open"], 2)
        self.assertEqual(result["totals"]["run_completed"], 3)


class RecoveryTests(unittest.TestCase):
    def test_checkpoint_resume_equals_single_run(self):
        events = base_flow()
        replayable, withdrawn, _ = resolve_events(events)
        ordered = order_events(
            [e for e in replayable if e["event_seconds"] <= 12],
            CONFIG["same_time_priority"],
        )
        single = CaseIndex(CONFIG["same_time_priority"])
        for event in ordered:
            single.apply(event)

        with tempfile.TemporaryDirectory() as directory:
            checkpoint = Path(directory) / "checkpoint.json"
            partial = CaseIndex(CONFIG["same_time_priority"])
            for event in ordered[:3]:
                partial.apply(event)
            partial.checkpoint(checkpoint)

            resumed = CaseIndex.load_checkpoint(checkpoint)
            for event in ordered[resumed.applied:]:
                resumed.apply(event)

        self.assertEqual(single.to_dict(), resumed.to_dict())


if __name__ == "__main__":
    unittest.main()
