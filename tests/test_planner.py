"""
Тесты детерминированного планировщика (app.planner).
"""
from __future__ import annotations

import pytest

from app.planner import plan_jobs, validate_no_collisions


def _jobs(*items: tuple[str, int, str, list[str]]) -> list[dict]:
    """(job_id, duration_s, material, allowed_printer_ids) -> list of job dicts."""
    return [
        {
            "job_id": jid,
            "duration_s": dur,
            "material": mat,
            "allowed_printer_ids": pids,
        }
        for jid, dur, mat, pids in items
    ]


def _printers(now: int, *items: tuple[str, str | None]) -> list[dict]:
    """(printer_id, current_material) -> list of printer dicts. available_at=now."""
    return [
        {"printer_id": pid, "available_at": now, "current_material": cur_mat}
        for pid, cur_mat in items
    ]


# --- 1) Детерминизм: один и тот же вход 10 раз → assignments идентичны ---
def test_determinism():
    now = 1000000
    jobs = _jobs(
        ("j1", 100, "PLA", ["p1", "p2"]),
        ("j2", 200, "ABS", ["p1", "p2"]),
        ("j3", 50, "PLA", ["p2"]),
    )
    printers = _printers(now, ("p1", None), ("p2", None))
    penalty = 500
    results = []
    for _ in range(10):
        assignments = plan_jobs(jobs, printers, now, penalty, log_trace=False)
        results.append([(a["job_id"], a["printer_id"], a["start"], a["end"]) for a in assignments])
    for i in range(1, len(results)):
        assert results[i] == results[0], f"Run {i} differs from run 0: {results[i]} vs {results[0]}"


# --- 2) Нет коллизий: валидатор не находит пересечений ---
def test_no_collisions():
    now = 2000000
    jobs = _jobs(
        ("a", 100, "M1", ["p1", "p2"]),
        ("b", 150, "M1", ["p1", "p2"]),
        ("c", 80, "M2", ["p1"]),
        ("d", 200, "M2", ["p2"]),
    )
    printers = _printers(now, ("p1", None), ("p2", None))
    assignments = plan_jobs(jobs, printers, now, 1000, log_trace=False)
    validate_no_collisions(assignments)
    by_printer: dict[str, list[dict]] = {}
    for a in assignments:
        pid = str(a["printer_id"])
        by_printer.setdefault(pid, []).append(a)
    for pid, lst in by_printer.items():
        lst.sort(key=lambda x: x["start"])
        for i in range(1, len(lst)):
            assert lst[i]["start"] >= lst[i - 1]["end"], (
                f"Printer {pid}: overlap {lst[i-1]} vs {lst[i]}"
            )


# --- 3) Материалы группируются: при повышении material_change_penalty смен меньше ---
def test_material_grouping():
    now = 3000000
    # 6 задач: 3 с материалом A, 3 с материалом B. 2 принтера.
    jobs = _jobs(
        ("j1", 100, "A", ["p1", "p2"]),
        ("j2", 100, "A", ["p1", "p2"]),
        ("j3", 100, "A", ["p1", "p2"]),
        ("j4", 100, "B", ["p1", "p2"]),
        ("j5", 100, "B", ["p1", "p2"]),
        ("j6", 100, "B", ["p1", "p2"]),
    )
    printers = _printers(now, ("p1", None), ("p2", None))

    plan_low = plan_jobs(jobs, printers, now, 0, log_trace=False)
    plan_high = plan_jobs(jobs, printers, now, 10000, log_trace=False)

    def count_material_changes(assignments: list[dict]) -> int:
        by_printer: dict[str, list[dict]] = {}
        for a in assignments:
            pid = str(a["printer_id"])
            by_printer.setdefault(pid, []).append(a)
        changes = 0
        for pid, lst in by_printer.items():
            lst.sort(key=lambda x: x["start"])
            prev_mat = None
            for a in lst:
                if prev_mat is not None and a["material"] != prev_mat:
                    changes += 1
                prev_mat = a["material"]
        return changes

    changes_low = count_material_changes(plan_low)
    changes_high = count_material_changes(plan_high)
    assert changes_high <= changes_low, (
        f"При высоком штрафе смен материала должно быть не больше: low={changes_low} high={changes_high}"
    )


def test_validate_no_collisions_raises_on_overlap():
    from app.planner import validate_no_collisions
    assignments = [
        {"job_id": "1", "printer_id": "p1", "start": 100, "end": 200},
        {"job_id": "2", "printer_id": "p1", "start": 150, "end": 250},
    ]
    with pytest.raises(ValueError, match="Коллизия"):
        validate_no_collisions(assignments)
