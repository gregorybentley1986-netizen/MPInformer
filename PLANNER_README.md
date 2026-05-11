# Детерминированный планировщик задач 3D-печати

Модуль `app.planner` строит расписание назначения задач на принтеры: один и тот же вход всегда даёт один и тот же план. Коллизии на одном принтере недопустимы; смена материала минимизируется через штраф. Между задачами на одном принтере по умолчанию выдерживается **зазор 15 минут** (один «пустой слот»).

## Вход

- **jobs** — список задач. Каждый элемент: `job_id`, `duration_s` (секунды, >0), `material`, опционально `priority`, `allowed_printer_ids` (список id принтеров, на которых можно печатать эту задачу). Печать задачи может быть назначена **только** на принтеры из `allowed_printer_ids`.
- **printers** — список принтеров. Каждый элемент: `printer_id`, `available_at` (Unix timestamp, секунды; когда принтер свободен), опционально `current_material`, `material_whitelist` (список допустимых материалов).
- **now** — текущее время (Unix timestamp). Явно передаётся для детерминизма.
- **material_change_penalty** — штраф в секундах за смену материала на принтере. Чем выше, тем сильнее планировщик группирует задачи с одним материалом на одном принтере.
- Опционально: **gap_after_s** — зазор в секундах после каждой задачи перед следующей на том же принтере (по умолчанию 900 секунд, то есть 15 минут).

## Выход

Список назначений (assignments), каждый элемент:

- `job_id`, `printer_id`, `start`, `end` (Unix timestamp), `material`, `score`, `change_penalty`

После построения плана вызывается `validate_no_collisions(assignments, gap_after_s)`; при обнаружении пересечений по времени на одном принтере возбуждается `ValueError`.

## Как подобрать material_change_penalty

Интерпретация: «сколько секунд мы готовы проиграть по времени, чтобы не менять материал».

- Если смена материала занимает ~15 минут → задайте **900** (секунд).
- Чтобы сильнее группировать материалы на одном принтере, увеличьте штраф до **2000–5000** секунд.
- При **0** планировщик не учитывает смену материала; при больших значениях почти всегда предпочитает тот же материал на принтере.

## Пример

```python
from app.planner import plan_jobs, validate_no_collisions

now = 1700000000  # Unix timestamp
jobs = [
    {"job_id": "j1", "duration_s": 3600, "material": "PLA", "allowed_printer_ids": ["p1", "p2"]},
    {"job_id": "j2", "duration_s": 1800, "material": "PLA", "allowed_printer_ids": ["p1", "p2"]},
    {"job_id": "j3", "duration_s": 7200, "material": "ABS", "allowed_printer_ids": ["p2"]},
]
printers = [
    {"printer_id": "p1", "available_at": now, "current_material": None},
    {"printer_id": "p2", "available_at": now, "current_material": None},
]

assignments = plan_jobs(jobs, printers, now, material_change_penalty=900, gap_after_s=900)
validate_no_collisions(assignments, gap_after_s=900)
# assignments: список { job_id, printer_id, start, end, material, score, change_penalty }
```

## Использование в приложении

При переносе плана в очередь («Перенести план в задачи») сервер вызывает детерминированный планировщик: строит из текущих данных списки `jobs` и `printers`, передаёт `now` (начало недели или следующий час от «сейчас» для текущей недели), `material_change_penalty=900` и `gap_after_s=900` (15 минут; можно не указывать явно, планировщик по умолчанию использует такой же зазор). Полученные назначения записываются в очередь без дополнительного выравнивания.

## Тесты

```bash
python -m pytest tests/test_planner.py -v
```

- **test_determinism** — один и тот же вход 10 раз → одинаковые assignments.
- **test_no_collisions** — после плана валидатор не находит пересечений.
- **test_material_grouping** — при увеличении material_change_penalty число смен материала не возрастает.
- **test_validate_no_collisions_raises_on_overlap** — при пересечении по времени валидатор возбуждает ValueError.
