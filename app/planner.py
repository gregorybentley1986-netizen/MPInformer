"""
Детерминированный планировщик задач 3D-печати.

Приоритет целей:
  1. Главная: максимально загрузить принтеры и быстрее завершить все задачи (минимум простоя, выравнивание).
  2. Второстепенная: уменьшить смену материала на принтерах.

Соответствие входных данных промпту (3d_print_scheduler_prompt.md) и нашим полям:
  Jobs:  job_id, duration_minutes|duration_s, material, compatible_models|allowed_printer_ids,
         priority, deadline, status — см. _normalize_job.
  Printers: printer_id, printer_model, current_material, available_from|available_at, status — см. _normalize_printer.
"""
from __future__ import annotations

import logging
from collections import defaultdict
from datetime import datetime, time, timedelta
from typing import Any

from dateutil import tz as dateutil_tz

logger = logging.getLogger(__name__)

MSK = dateutil_tz.gettz("Europe/Moscow")

WORKDAY_START = time(8, 0)
WORKDAY_END = time(20, 0)
DEFAULT_GAP_AFTER_S = 15 * 60

# Цель 2 главная, цель 1 второстепенная: в cost доминирует задержка старта (загрузка/быстрее завершить).
# cost = material_change_penalty * смена + makespan_weight * (start_ts - now)
# 1 смена материала = ONE_CHANGE_EQUIVALENT_HOURS ч задержки → при 24 ч задержка решает выбор.
ONE_CHANGE_EQUIVALENT_HOURS = 24.0  # 1 смена ≈ 24 ч задержки → цель 2 (загрузка/быстрее) главная
W_MATERIAL_CHANGE_DEFAULT = 1.0

# Выравнивание нагрузки: разница в окончании работ между принтерами не более этого (сек).
REBALANCE_MAX_IMBALANCE_SEC = 24 * 3600  # сутки

# Защита от зависания: макс. итераций в циклах перераспределения (rebalance и minimize_makespan).
MAX_REBALANCE_ITER = 5000
MAX_MINIMIZE_MAKESPAN_ITER = 5000


def _next_allowed_start(ts: int) -> int:
    """Сдвигает ts вперёд до начала разрешённого окна (8:00–20:00 MSK)."""
    try:
        dt = datetime.fromtimestamp(int(ts), tz=MSK)
    except Exception:
        return int(ts)
    cur_t = dt.timetz()
    if cur_t < WORKDAY_START:
        dt = dt.replace(hour=WORKDAY_START.hour, minute=WORKDAY_START.minute, second=0, microsecond=0)
    elif cur_t >= WORKDAY_END:
        next_day = dt + timedelta(days=1)
        dt = next_day.replace(hour=WORKDAY_START.hour, minute=WORKDAY_START.minute, second=0, microsecond=0)
    return int(dt.timestamp())


def _normalize_id(x: Any) -> str:
    return str(x) if x is not None else ""


def _datetime_to_ts(value: Any) -> int | None:
    """Преобразует datetime или timestamp в int (секунды)."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return int(value)
    if hasattr(value, "timestamp"):
        return int(value.timestamp())
    return None


def _normalize_job(job: dict[str, Any], all_printer_ids: list[str], printers_by_id: dict[str, dict]) -> dict[str, Any]:
    """
    Приводит задание к единому виду.
    Промпт: job_id, duration_minutes, material, compatible_models, priority, deadline.
    У нас: job_id, duration_s, material, allowed_printer_ids.
    """
    out = dict(job)
    # duration_s: из duration_minutes (промпт) или duration_s (наши)
    dur_s = job.get("duration_s")
    if dur_s is None and "duration_minutes" in job:
        mn = job.get("duration_minutes") or 0
        out["duration_s"] = int(mn) * 60
    else:
        out["duration_s"] = int(dur_s or 0)
    # eligible_printer_ids: из compatible_models + printer_model + status (промпт) или allowed_printer_ids (наши)
    compatible = job.get("compatible_models")
    if compatible is not None and isinstance(compatible, (list, set)):
        compatible_set = {_normalize_id(m) for m in compatible}
        eligible = [
            pid for pid in all_printer_ids
            if _normalize_id(printers_by_id.get(pid, {}).get("printer_model")) in compatible_set
            and printers_by_id.get(pid, {}).get("status", "available") == "available"
        ]
    else:
        allowed = job.get("allowed_printer_ids")
        if allowed is not None:
            eligible = [_normalize_id(p) for p in allowed]
        else:
            eligible = list(all_printer_ids)
    out["eligible_printer_ids"] = eligible
    out["material"] = job.get("material")
    out["priority"] = int(job.get("priority") or 0)
    out["deadline_ts"] = _datetime_to_ts(job.get("deadline"))
    return out


def _normalize_printers(printers: list[dict[str, Any]], now: int) -> tuple[list[dict[str, Any]], list[str], dict[str, dict]]:
    """
    Фильтрует по status=available (если передан status), приводит available_from → available_at.
    Промпт: available_from (datetime), status. У нас: available_at (timestamp).
    """
    result = []
    for p in printers:
        if p.get("status") not in (None, "", "available"):
            continue
        at = p.get("available_at")
        if at is None and "available_from" in p:
            at = _datetime_to_ts(p["available_from"])
        if at is None:
            at = now
        else:
            at = max(int(at), now)
        rec = dict(p)
        rec["available_at"] = at
        rec["current_material"] = _normalize_id(p.get("current_material"))
        result.append(rec)
    result.sort(key=lambda x: _normalize_id(x.get("printer_id")))
    ids = [_normalize_id(p.get("printer_id")) for p in result]
    by_id = {_normalize_id(p.get("printer_id")): p for p in result}
    return result, ids, by_id


def _rebalance_assignments(
    assignments: list[dict[str, Any]],
    job_id_to_eligible: dict[Any, set[str]],
    all_printer_ids: list[str],
    printer_by_id: dict[str, dict],
    gap_after_s: int,
    now: int,
    max_imbalance_sec: int = REBALANCE_MAX_IMBALANCE_SEC,
) -> None:
    """
    Выравнивание: принтер «Свободен» (раньше всех освободился или без задач) забирает задачи
    с наиболее загруженного (только подходящие по eligible). Повторяем, пока разница в пределах суток.
    """
    if not assignments or not all_printer_ids:
        return
    by_printer: dict[str, list[dict]] = defaultdict(list)
    for a in assignments:
        pid = _normalize_id(a.get("printer_id"))
        by_printer[pid].append(a)
    for pid in all_printer_ids:
        if pid in by_printer:
            by_printer[pid].sort(key=lambda x: int(x.get("start", 0)))

    def last_end_for(pid: str) -> int:
        lst = by_printer.get(pid, [])
        return lst[-1]["end"] if lst else now

    iter_count = 0
    while iter_count < MAX_REBALANCE_ITER:
        iter_count += 1
        last_ends = {pid: last_end_for(pid) for pid in all_printer_ids}
        free_pid = min(last_ends, key=last_ends.get)
        busy_pid = max(last_ends, key=last_ends.get)
        if last_ends[busy_pid] - last_ends[free_pid] <= max_imbalance_sec:
            break
        busy_list = by_printer.get(busy_pid, [])
        if not busy_list:
            break
        moved = False
        for i in range(len(busy_list) - 1, -1, -1):
            a = busy_list[i]
            jid = a.get("job_id")
            eligible = job_id_to_eligible.get(jid, set())
            if free_pid not in eligible:
                continue
            duration = int(a.get("end", 0)) - int(a.get("start", 0))
            if duration <= 0:
                continue
            free_last_end = last_ends[free_pid]
            new_start = _next_allowed_start(free_last_end + gap_after_s)
            new_end = new_start + duration
            a["printer_id"] = printer_by_id[free_pid].get("printer_id")
            a["start"] = new_start
            a["end"] = new_end
            busy_list.pop(i)
            if free_pid not in by_printer:
                by_printer[free_pid] = []
            by_printer[free_pid].append(a)
            by_printer[free_pid].sort(key=lambda x: int(x.get("start", 0)))
            moved = True
            break
        if not moved:
            break


def _minimize_makespan_phase(
    assignments: list[dict[str, Any]],
    job_id_to_eligible: dict[Any, set[str]],
    all_printer_ids: list[str],
    printer_by_id: dict[str, dict],
    gap_after_s: int,
    now: int,
) -> None:
    """
    Финальная фаза: перераспределяем хвосты на освободившиеся принтеры.
    Перенос только при строгом улучшении (new_makespan < current), чтобы не зациклиться.
    Приоритет: принтер «Свободен», затем тот же материал. Лимит итераций — защита от зависания.
    """
    if not assignments or not all_printer_ids:
        return
    by_printer: dict[str, list[dict]] = defaultdict(list)
    for a in assignments:
        pid = _normalize_id(a.get("printer_id"))
        by_printer[pid].append(a)
    for pid in all_printer_ids:
        if pid in by_printer:
            by_printer[pid].sort(key=lambda x: int(x.get("start", 0)))

    def last_end_for(pid: str) -> int:
        lst = by_printer.get(pid, [])
        return lst[-1]["end"] if lst else now

    def current_makespan() -> int:
        return max(last_end_for(pid) for pid in all_printer_ids)

    iter_count = 0
    while iter_count < MAX_MINIMIZE_MAKESPAN_ITER:
        iter_count += 1
        last_ends = {pid: last_end_for(pid) for pid in all_printer_ids}
        busy_pid = max(last_ends, key=last_ends.get)
        busy_list = by_printer.get(busy_pid, [])
        if not busy_list:
            break
        current_ms = current_makespan()
        # Собираем все улучшающие переносы; приоритет: принтер «Свободен» (очередь пуста), затем тот же материал
        candidates = []
        for idx in range(len(busy_list) - 1, -1, -1):
            task = busy_list[idx]
            jid = task.get("job_id")
            eligible = job_id_to_eligible.get(jid, set())
            duration = int(task.get("end", 0)) - int(task.get("start", 0))
            if duration <= 0:
                continue
            busy_new_end = busy_list[idx - 1]["end"] if idx > 0 else now
            task_mat = _normalize_id(task.get("material"))
            for target_pid in all_printer_ids:
                if target_pid == busy_pid or target_pid not in eligible:
                    continue
                target_list = by_printer.get(target_pid, [])
                target_free = 1 if not target_list else 0
                target_last = target_list[-1]["end"] if target_list else now
                target_last_mat = _normalize_id(target_list[-1].get("material")) if target_list else ""
                new_start = _next_allowed_start(target_last + gap_after_s)
                new_end = new_start + duration
                new_ends = {p: last_end_for(p) for p in all_printer_ids}
                new_ends[busy_pid] = busy_new_end
                new_ends[target_pid] = new_end
                new_makespan = max(new_ends.values())
                if new_makespan < current_ms:
                    same_material = 1 if task_mat == target_last_mat else 0
                    candidates.append((new_makespan, target_free, same_material, idx, target_pid))
        if not candidates:
            break
        # Лучший: меньше makespan, затем принтер Свободен, затем тот же материал
        candidates.sort(key=lambda c: (c[0], -c[1], -c[2]))
        _, _, _, best_idx, best_target_pid = candidates[0]
        target_pid = best_target_pid
        task = busy_list.pop(best_idx)
        target_list = by_printer.get(target_pid, [])
        target_last = target_list[-1]["end"] if target_list else now
        duration = int(task.get("end", 0)) - int(task.get("start", 0))
        new_start = _next_allowed_start(target_last + gap_after_s)
        new_end = new_start + duration
        task["printer_id"] = printer_by_id[target_pid].get("printer_id")
        task["start"] = new_start
        task["end"] = new_end
        if target_pid not in by_printer:
            by_printer[target_pid] = []
        by_printer[target_pid].append(task)
        by_printer[target_pid].sort(key=lambda x: int(x.get("start", 0)))


def plan_jobs(
    jobs: list[dict[str, Any]],
    printers: list[dict[str, Any]],
    now: int,
    material_change_penalty: int,
    *,
    gap_after_s: int = DEFAULT_GAP_AFTER_S,
    objective: str = "min_makespan",
    log_trace: bool = True,
    makespan_weight: float | None = None,
) -> list[dict[str, Any]]:
    """
    Цель 2 главная (загрузка, быстрее завершить), цель 1 второстепенная (меньше смен материала).
    Phase 1–2 — eligible, сортировка. Phase 3–4 — заполнение; cost = penalty * смена + makespan_weight * задержка.
    По умолчанию makespan_weight = penalty / (ONE_CHANGE_EQUIVALENT_HOURS * 3600); при 24 ч одна смена ≈ сутки задержки.
    """
    if makespan_weight is None:
        eq_sec = ONE_CHANGE_EQUIVALENT_HOURS * 3600
        makespan_weight = material_change_penalty / eq_sec if (eq_sec and material_change_penalty) else 1.0
    if not printers:
        return []
    printers_norm, all_printer_ids, printer_by_id = _normalize_printers(printers, now)
    if not all_printer_ids:
        return []

    # Нормализация заданий и отбор по длительности
    jobs_norm = [_normalize_job(j, all_printer_ids, printer_by_id) for j in jobs]
    jobs_norm = [j for j in jobs_norm if j["duration_s"] > 0]
    if not jobs_norm:
        return []

    # Phase 1: невозможные (нет подходящих принтеров)
    impossible = [j for j in jobs_norm if not j["eligible_printer_ids"]]
    schedulable = [j for j in jobs_norm if j["eligible_printer_ids"]]
    if impossible and log_trace:
        logger.debug("Planner: impossible jobs (no eligible printer): %s", [j.get("job_id") for j in impossible])

    # Phase 2: сортировка — deadline (раньше первые), flexibility (меньше первые), priority (выше первые), duration (короче первые)
    BIG = 2**31
    def sort_key(j):
        dl = j.get("deadline_ts") or BIG
        flex = len(j["eligible_printer_ids"])
        prio = -(j.get("priority") or 0)
        dur = j["duration_s"]
        return (dl, flex, prio, dur, _normalize_id(j.get("job_id")))
    schedulable.sort(key=sort_key)
    job_id_to_eligible = {
        j["job_id"]: set(_normalize_id(p) for p in j["eligible_printer_ids"])
        for j in schedulable
    }

    # Состояние принтеров: available_at (ts), current_material (str)
    state: dict[str, int] = {}
    current_mat: dict[str, str] = {}
    for p in printers_norm:
        pid = _normalize_id(p.get("printer_id"))
        state[pid] = _next_allowed_start(p["available_at"])
        current_mat[pid] = _normalize_id(p.get("current_material"))

    assignments: list[dict[str, Any]] = []
    unscheduled = list(schedulable)
    job_to_eligible = {id(j): set(j["eligible_printer_ids"]) for j in unscheduled}

    while unscheduled:
        progress = False
        # Phase 3: только тот же материал
        for pid in sorted(all_printer_ids, key=lambda p: state[p]):
            mat = current_mat.get(pid, "")
            candidates = [
                j for j in unscheduled
                if pid in job_to_eligible.get(id(j), set())
                and _normalize_id(j.get("material")) == mat
            ]
            if not candidates:
                continue
            best = min(candidates, key=lambda j: (
                state[pid],
                sort_key(j),
            ))
            start_ts = _next_allowed_start(state[pid])
            end_ts = start_ts + best["duration_s"]
            state[pid] = end_ts + gap_after_s
            current_mat[pid] = _normalize_id(best.get("material"))
            assignments.append({
                "job_id": best.get("job_id"),
                "printer_id": printer_by_id[pid].get("printer_id"),
                "start": start_ts,
                "end": end_ts,
                "material": best.get("material"),
                "score": end_ts,
                "change_penalty": 0,
            })
            unscheduled.remove(best)
            progress = True
            break
        if progress:
            continue

        # Phase 4: смена материала — минимизируем cost: смена материала + задержка старта
        best_assign = None
        best_cost = None
        for pid in sorted(all_printer_ids, key=lambda p: state[p]):
            candidates = [j for j in unscheduled if pid in job_to_eligible.get(id(j), set())]
            if not candidates:
                continue
            start_ts = _next_allowed_start(state[pid])
            delay_s = max(0, start_ts - now)
            for j in candidates:
                end_ts = start_ts + j["duration_s"]
                change = 1 if _normalize_id(j.get("material")) != current_mat.get(pid, "") else 0
                cost = change * material_change_penalty * W_MATERIAL_CHANGE_DEFAULT + makespan_weight * delay_s
                if best_cost is None or cost < best_cost:
                    best_cost = cost
                    best_assign = (pid, j, start_ts, end_ts)
        if best_assign is None:
            break
        pid, best, start_ts, end_ts = best_assign
        prev_mat = current_mat.get(pid) or ""
        state[pid] = end_ts + gap_after_s
        current_mat[pid] = _normalize_id(best.get("material"))
        assignments.append({
            "job_id": best.get("job_id"),
            "printer_id": printer_by_id[pid].get("printer_id"),
            "start": start_ts,
            "end": end_ts,
            "material": best.get("material"),
            "score": end_ts,
            "change_penalty": 1 if _normalize_id(best.get("material")) != prev_mat else 0,
        })
        unscheduled.remove(best)
        progress = True

    # Выравнивание: свободный принтер забирает задачи с наиболее загруженного (в пределах суток)
    _rebalance_assignments(
        assignments,
        job_id_to_eligible,
        all_printer_ids,
        printer_by_id,
        gap_after_s,
        now,
        max_imbalance_sec=REBALANCE_MAX_IMBALANCE_SEC,
    )
    # Финальная фаза: перекидываем задачи с самых загруженных на освободившиеся
    _minimize_makespan_phase(
        assignments,
        job_id_to_eligible,
        all_printer_ids,
        printer_by_id,
        gap_after_s,
        now,
    )
    validate_no_collisions(assignments, gap_after_s=gap_after_s)
    return assignments


def validate_no_collisions(assignments: list[dict[str, Any]], *, gap_after_s: int = 0) -> None:
    """Проверяет отсутствие пересечений по времени на одном принтере."""
    by_printer: dict[str, list] = defaultdict(list)
    for a in assignments:
        pid = _normalize_id(a.get("printer_id"))
        by_printer[pid].append(a)
    for pid, lst in by_printer.items():
        lst_sorted = sorted(lst, key=lambda x: int(x.get("start", 0)))
        for i in range(1, len(lst_sorted)):
            prev_end = int(lst_sorted[i - 1].get("end", 0))
            min_next = prev_end + gap_after_s
            cur_start = int(lst_sorted[i].get("start", 0))
            if cur_start < min_next:
                raise ValueError(
                    f"Коллизия на принтере {pid}: start={cur_start} < {min_next} (prev end={prev_end} + gap={gap_after_s})"
                )
