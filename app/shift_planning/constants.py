"""Константы листов-заданий на смену."""

SHIFT_SHEET_STATUS_DRAFT = "draft"
SHIFT_SHEET_STATUS_PUBLISHED = "published"
SHIFT_SHEET_STATUS_CLOSED = "closed"

SHIFT_SHEET_STATUS_LABELS = {
    SHIFT_SHEET_STATUS_DRAFT: "Черновик",
    SHIFT_SHEET_STATUS_PUBLISHED: "Выдан оператору",
    SHIFT_SHEET_STATUS_CLOSED: "Закрыт",
}

SHIFT_TASK_TYPE_PRINT = "print"
SHIFT_TASK_TYPE_ASSEMBLE = "assemble"
SHIFT_TASK_TYPE_PACK = "pack"

SHIFT_TASK_TYPE_LABELS = {
    SHIFT_TASK_TYPE_PRINT: "Печать деталей",
    SHIFT_TASK_TYPE_ASSEMBLE: "Сборка изделий",
    SHIFT_TASK_TYPE_PACK: "Упаковка",
}

SHIFT_TASK_STATUS_PENDING = "pending"
SHIFT_TASK_STATUS_COMPLETED = "completed"
SHIFT_TASK_STATUS_PARTIAL = "partial"
SHIFT_TASK_STATUS_FAILED = "failed"

SHIFT_TASK_STATUS_LABELS = {
    SHIFT_TASK_STATUS_PENDING: "Не отмечено",
    SHIFT_TASK_STATUS_COMPLETED: "Выполнено",
    SHIFT_TASK_STATUS_PARTIAL: "Частично",
    SHIFT_TASK_STATUS_FAILED: "Не выполнено",
}

USER_ROLE_STAFF = "staff"
USER_ROLE_OPERATOR = "operator"

USER_ROLE_LABELS = {
    USER_ROLE_STAFF: "Сотрудник (полный доступ)",
    USER_ROLE_OPERATOR: "Оператор смены",
}
