# Prompt for AI Coding Agent: 3D Printing Job Scheduler

You are developing a production scheduler for distributing 3D‑printing
jobs across printers.

## Goal

Minimize material changes while respecting: - printer compatibility -
non‑overlapping schedules - priorities and deadlines - minimal idle
time - protection of scarce printers

------------------------------------------------------------------------

# 1. Input Data

Соответствие полей в нашем проекте (app/planner.py): промпт принимает оба варианта имён.
- **Jobs**: job_id, duration_minutes **или** duration_s, material, compatible_models **или** allowed_printer_ids (список id принтеров), priority, deadline.
- **Printers**: printer_id, printer_model (для совместимости по compatible_models), current_material, available_from **или** available_at (timestamp), status (если не "available" — принтер не участвует).

## Jobs

Fields: - job_id: str - duration_minutes: int - material: str -
compatible_models: list\[str\] - priority: int \| None - deadline:
datetime \| None - created_at: datetime \| None - quantity: int \|
None - gcode_id: str \| None

## Printers

Fields: - printer_id: str - printer_model: str - current_material: str
\| None - available_from: datetime - status: str

------------------------------------------------------------------------

# 2. Compatibility

Eligible printers:

eligible_printers(job) = { p ∈ printers \| p.printer_model ∈
job.compatible_models AND p.status = available }

If eligible_printers = empty → job goes to impossible_jobs.

------------------------------------------------------------------------

# 3. Deterministic Planner

The scheduler must be deterministic: - no randomness - same input → same
schedule - algorithmic heuristic approach

Primary optimization goal: minimize material changes.

------------------------------------------------------------------------

# 4. Global Objective Function

score = w1 \* material_changes + w2 \* idle_time + w3 \* lateness + w4
\* load_imbalance

Recommended weights:

w1 = 1000 w2 = 1 w3 = 100 w4 = 5

------------------------------------------------------------------------

# 5. Assignment Cost Function

cost(job, printer) = 1000 \* material_change + 1 \* waiting_time + 50 \*
lateness + 20 \* flexibility_penalty

Extended version:

cost = 1000 \* change_material + 2 \* start_delay + 200 \*
deadline_violation + 50 \* scarce_printer_penalty - 100 \*
same_material_batch_bonus

Where: change_material = 1 if material differs else 0

deadline_violation = max(0, finish_time - deadline)

Minimize cost.

------------------------------------------------------------------------

# 6. Job Flexibility

flexibility(job) = \|eligible_printers(job)\|

Lower flexibility jobs must be scheduled earlier.

------------------------------------------------------------------------

# 7. Material Selection Score

material_score = 3 \* total_duration + 2 \* jobs_count + 5 \*
urgent_jobs - 10 \* is_material_change - 2 \* flexibility

Choose material with highest score.

------------------------------------------------------------------------

# 8. Material Switching Benefit

benefit(material, printer) = α \* total_processable_time + β \*
jobs_count - γ \* changeover_penalty - δ \* urgency_loss

Suggested coefficients:

α = 3 β = 2 γ = 10 δ = 5

Switch material only if benefit \> 0.

------------------------------------------------------------------------

# 9. Scheduling Phases

Phase 1 --- Preprocessing - compute eligible printers - detect
impossible jobs

Phase 2 --- Sort critical jobs Sort by: 1. earliest deadline 2. lowest
flexibility 3. highest priority 4. shortest duration

Phase 3 --- Fill printers without material change

Phase 4 --- Material batch switching

Phase 5 --- Local optimization - move single jobs - reduce material
changes - improve packing

------------------------------------------------------------------------

# 10. Constraints

1.  Jobs cannot overlap on a printer.
2.  Job must run only on compatible printers.
3.  Offline printers ignored.
4.  Deadlines penalized.
5.  Deterministic results.

------------------------------------------------------------------------

# 11. Architecture

Language: Python

Modules: models.py scheduler.py scoring.py optimizer.py main.py

Use dataclasses:

Job Printer Assignment ScheduleResult

------------------------------------------------------------------------

# 12. Core Pseudocode

def schedule_jobs(jobs, printers):

    for job in jobs:
        job.eligible_printers = [
            p for p in printers
            if p.printer_model in job.compatible_models and p.status == "available"
        ]

    impossible_jobs = [j for j in jobs if not j.eligible_printers]
    unscheduled = [j for j in jobs if j.eligible_printers]
    assignments = []

    unscheduled = sorted(
        unscheduled,
        key=lambda j: (
            deadline_rank(j),
            len(j.eligible_printers),
            -priority_rank(j),
            j.duration_minutes
        )
    )

    while unscheduled:

        progress = False

        for printer in sorted(printers, key=lambda p: p.available_from):

            candidates = get_candidates(
                jobs=unscheduled,
                printer=printer,
                same_material_only=True
            )

            if candidates:
                job = choose_best_candidate(candidates, printer)
                assign(job, printer, assignments)
                unscheduled.remove(job)
                progress = True

        if progress:
            continue

        for printer in sorted(printers, key=lambda p: p.available_from):

            material_groups = group_candidates_by_material(unscheduled, printer)
            best_material = choose_best_material(material_groups, printer)

            if best_material is not None:

                job = choose_best_candidate(
                    material_groups[best_material], printer
                )

                switch_material_if_needed(printer, best_material)

                assign(job, printer, assignments)
                unscheduled.remove(job)
                progress = True

        if not progress:
            break

    assignments = improve_schedule(assignments, printers)

    return build_result(assignments, impossible_jobs, unscheduled)

------------------------------------------------------------------------

# 13. Output

Assignments list: job_id printer_id start_time end_time material_before
material_after material_change

Also return:

impossible_jobs unassigned_jobs

Statistics:

total_jobs scheduled_jobs material_changes idle_minutes lateness_minutes
load_by_printer

------------------------------------------------------------------------

# 14. Requirements

Produce: - full working Python implementation - project structure -
example dataset - example run - documented coefficients - comments in
code - no TODO placeholders

------------------------------------------------------------------------

# 15. Important

Do NOT use LLM logic for scheduling. Use deterministic heuristic
optimization.
