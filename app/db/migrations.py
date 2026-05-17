"""
Миграции схемы БД (добавление колонок и т.п.).
"""
from sqlalchemy import text


def ensure_part_stl_thumb(conn):
    """Добавить колонку stl_thumb_filename в parts, если её нет."""
    try:
        conn.execute(text(
            "ALTER TABLE parts ADD COLUMN stl_thumb_filename VARCHAR(512) NOT NULL DEFAULT ''"
        ))
    except Exception:
        pass


def ensure_product_part_material_id(conn):
    """Добавить колонку material_id в product_parts, если её нет."""
    try:
        conn.execute(text("ALTER TABLE product_parts ADD COLUMN material_id INTEGER"))
    except Exception:
        pass


def ensure_product_article(conn):
    """Добавить колонку article в products, если её нет."""
    try:
        conn.execute(text(
            "ALTER TABLE products ADD COLUMN article VARCHAR(128) NOT NULL DEFAULT ''"
        ))
    except Exception:
        pass


def ensure_print_jobs_table(conn):
    """Создать таблицу print_jobs, если её нет."""
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS print_jobs (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            name VARCHAR(256) NOT NULL DEFAULT '',
            part_quantities TEXT NOT NULL DEFAULT '[]',
            printer_ids TEXT NOT NULL DEFAULT '[]',
            execution_time VARCHAR(128) NOT NULL DEFAULT '',
            material_weight_grams REAL NOT NULL DEFAULT 0.0,
            gcode_filename VARCHAR(512) NOT NULL DEFAULT '',
            gcode_thumb_filename VARCHAR(512) NOT NULL DEFAULT ''
        )
    """))


def ensure_print_job_gcode_thumb(conn):
    """Добавить колонку gcode_thumb_filename в print_jobs, если её нет."""
    try:
        conn.execute(text(
            "ALTER TABLE print_jobs ADD COLUMN gcode_thumb_filename VARCHAR(512) NOT NULL DEFAULT ''"
        ))
    except Exception:
        pass


def ensure_print_queue_items_table(conn):
    """Создать таблицу print_queue_items, если её нет."""
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS print_queue_items (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            sequence INTEGER NOT NULL DEFAULT 0,
            print_job_id INTEGER NOT NULL REFERENCES print_jobs(id) ON DELETE CASCADE,
            printer_id INTEGER NOT NULL REFERENCES printers(id) ON DELETE CASCADE,
            material_id INTEGER REFERENCES materials(id) ON DELETE SET NULL,
            scheduled_start TIMESTAMP NOT NULL
        )
    """))


def ensure_print_queue_items_sequence(conn):
    """Добавить колонку sequence в print_queue_items, если её нет."""
    try:
        conn.execute(text(
            "ALTER TABLE print_queue_items ADD COLUMN sequence INTEGER NOT NULL DEFAULT 0"
        ))
    except Exception:
        pass


def ensure_print_plans_table(conn):
    """Создать таблицу print_plans (план печати на неделю, хранение 6 мес)."""
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS print_plans (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            week_start VARCHAR(10) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """))
    try:
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_print_plans_week_start ON print_plans (week_start)"))
    except Exception:
        pass


def ensure_print_plan_items_table(conn):
    """Создать таблицу print_plan_items."""
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS print_plan_items (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            print_plan_id INTEGER NOT NULL REFERENCES print_plans(id) ON DELETE CASCADE,
            product_id INTEGER NOT NULL REFERENCES products(id) ON DELETE CASCADE,
            quantity INTEGER NOT NULL DEFAULT 1
        )
    """))


def ensure_spools_table(conn):
    """Создать таблицу spools (регистр катушек: материал, остаток в метрах)."""
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS spools (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            material_id INTEGER REFERENCES materials(id) ON DELETE SET NULL,
            remaining_length_m REAL NOT NULL DEFAULT 0.0
        )
    """))


def ensure_printers_current_spool_id(conn):
    """Добавить колонку current_spool_id в printers, если её нет."""
    try:
        conn.execute(text(
            "ALTER TABLE printers ADD COLUMN current_spool_id INTEGER REFERENCES spools(id) ON DELETE SET NULL"
        ))
    except Exception:
        pass


def ensure_product_barcode_columns(conn):
    """Добавить колонки штрихкодов Озон и Вайлдберрис в products, если их нет."""
    for col in ("ozon_barcode_filename", "wildberries_barcode_filename"):
        try:
            conn.execute(text(
                f"ALTER TABLE products ADD COLUMN {col} VARCHAR(512) NOT NULL DEFAULT ''"
            ))
        except Exception:
            pass


def ensure_product_ozon_sku(conn):
    """Добавить колонку ozon_sku в products, если её нет."""
    try:
        conn.execute(text(
            "ALTER TABLE products ADD COLUMN ozon_sku INTEGER NULL"
        ))
    except Exception:
        pass


def ensure_product_wildberries_sku(conn):
    """Добавить колонку wildberries_sku в products, если её нет."""
    try:
        conn.execute(text(
            "ALTER TABLE products ADD COLUMN wildberries_sku INTEGER NULL"
        ))
    except Exception:
        pass


def ensure_packaging_tasks_tables(conn):
    """Создать таблицы упаковочных заданий."""
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS packaging_tasks (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            number VARCHAR(64) NOT NULL DEFAULT '',
            marketplace VARCHAR(64) NOT NULL DEFAULT '',
            delivery_number VARCHAR(128) NOT NULL DEFAULT '',
            status VARCHAR(32) NOT NULL DEFAULT 'created'
        )
    """))
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS packaging_task_items (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            packaging_task_id INTEGER NOT NULL REFERENCES packaging_tasks(id) ON DELETE CASCADE,
            product_id INTEGER NOT NULL REFERENCES products(id) ON DELETE CASCADE,
            quantity INTEGER NOT NULL DEFAULT 1
        )
    """))


def ensure_supply_queue_result_day_counts(conn):
    """Добавить колонку day_counts в supply_queue_results, если её нет."""
    try:
        conn.execute(text(
            "ALTER TABLE supply_queue_results ADD COLUMN day_counts TEXT"
        ))
    except Exception:
        pass


def ensure_ozon_supplies_table(conn):
    """Таблица заявок на поставку Ozon (созданных через черновик)."""
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS ozon_supplies (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            ozon_supply_id VARCHAR(64) NOT NULL DEFAULT '',
            posting_number VARCHAR(32) NOT NULL DEFAULT '',
            destination_warehouse VARCHAR(256) NOT NULL DEFAULT '',
            shipment_date VARCHAR(32) NOT NULL DEFAULT '',
            delivery_date_estimated VARCHAR(32) NOT NULL DEFAULT '',
            composition TEXT NOT NULL DEFAULT '[]',
            status VARCHAR(64) NOT NULL DEFAULT 'created',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """))
    try:
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_ozon_supplies_ozon_supply_id ON ozon_supplies (ozon_supply_id)"))
    except Exception:
        pass
    try:
        conn.execute(text("ALTER TABLE ozon_supplies ADD COLUMN posting_number VARCHAR(32) NOT NULL DEFAULT ''"))
    except Exception:
        pass
    try:
        conn.execute(text("ALTER TABLE ozon_supplies ADD COLUMN destination_warehouse VARCHAR(256) NOT NULL DEFAULT ''"))
    except Exception:
        pass
    try:
        conn.execute(text("ALTER TABLE ozon_supplies ADD COLUMN has_cargo_places INTEGER NOT NULL DEFAULT 0"))
    except Exception:
        pass
    try:
        conn.execute(text("ALTER TABLE ozon_supplies ADD COLUMN cargo_places_status VARCHAR(32) NOT NULL DEFAULT ''"))
    except Exception:
        pass
    try:
        conn.execute(text("ALTER TABLE ozon_supplies ADD COLUMN cargo_places_data TEXT NOT NULL DEFAULT '[]'"))
    except Exception:
        pass


def ensure_warehouse_extra_stock_table(conn):
    """Таблица остатков дополнительных материалов на складе."""
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS warehouse_extra_stock (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            extra_material_id INTEGER NOT NULL REFERENCES extra_materials(id) ON DELETE CASCADE,
            quantity INTEGER NOT NULL DEFAULT 0,
            UNIQUE(extra_material_id)
        )
    """))


def ensure_written_off_materials_table(conn):
    """Таблица списанных материалов (катушки и доп. материалы)."""
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS written_off_materials (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            item_type VARCHAR(16) NOT NULL,
            spool_id INTEGER REFERENCES spools(id) ON DELETE SET NULL,
            material_id INTEGER REFERENCES materials(id) ON DELETE SET NULL,
            extra_material_id INTEGER REFERENCES extra_materials(id) ON DELETE SET NULL,
            display_name VARCHAR(512) NOT NULL DEFAULT '',
            quantity INTEGER NOT NULL DEFAULT 1,
            written_off_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """))


def ensure_printed_part_stock_table(conn):
    """Таблица остатков напечатанных деталей на складе."""
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS printed_part_stock (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            part_id INTEGER NOT NULL REFERENCES parts(id) ON DELETE CASCADE,
            material_id INTEGER REFERENCES materials(id) ON DELETE SET NULL,
            quantity INTEGER NOT NULL DEFAULT 0
        )
    """))
    # Миграция с версии, где был только part_id и UNIQUE(part_id):
    # пересоздаём таблицу в нужной структуре, чтобы хранить одну и ту же деталь в разных материалах.
    try:
        cols = conn.execute(text("PRAGMA table_info(printed_part_stock)")).fetchall()
        col_names = {str(c[1]) for c in cols}
    except Exception:
        col_names = set()
    needs_rebuild = "material_id" not in col_names
    if needs_rebuild:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS printed_part_stock_new (
                id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                part_id INTEGER NOT NULL REFERENCES parts(id) ON DELETE CASCADE,
                material_id INTEGER REFERENCES materials(id) ON DELETE SET NULL,
                quantity INTEGER NOT NULL DEFAULT 0
            )
        """))
        conn.execute(text("""
            INSERT INTO printed_part_stock_new (id, part_id, material_id, quantity)
            SELECT id, part_id, NULL, quantity
            FROM printed_part_stock
        """))
        conn.execute(text("DROP TABLE printed_part_stock"))
        conn.execute(text("ALTER TABLE printed_part_stock_new RENAME TO printed_part_stock"))
    conn.execute(text(
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_printed_part_stock_part_material "
        "ON printed_part_stock(part_id, material_id)"
    ))


def ensure_assembled_product_stock_table(conn):
    """Таблица остатков собранных изделий на складе."""
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS assembled_product_stock (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            product_id INTEGER NOT NULL REFERENCES products(id) ON DELETE CASCADE,
            quantity INTEGER NOT NULL DEFAULT 0,
            UNIQUE(product_id)
        )
    """))


def ensure_assembled_product_stock_log_table(conn):
    """Журнал движений собранных изделий."""
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS assembled_product_stock_log (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            product_id INTEGER REFERENCES products(id) ON DELETE SET NULL,
            product_label VARCHAR(512) NOT NULL DEFAULT '',
            delta_qty INTEGER NOT NULL,
            action_kind VARCHAR(32) NOT NULL,
            assembly_batch_id INTEGER REFERENCES warehouse_assembly_batches(id) ON DELETE SET NULL
        )
    """))
    try:
        conn.execute(text(
            "CREATE INDEX IF NOT EXISTS ix_assembled_product_stock_log_created_at "
            "ON assembled_product_stock_log (created_at)"
        ))
    except Exception:
        pass


def ensure_warehouse_defect_records_table(conn):
    """Таблица журнала брака (детали и изделия)."""
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS warehouse_defect_records (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            item_type VARCHAR(16) NOT NULL,
            printed_stock_id INTEGER REFERENCES printed_part_stock(id) ON DELETE SET NULL,
            part_id INTEGER REFERENCES parts(id) ON DELETE SET NULL,
            product_id INTEGER REFERENCES products(id) ON DELETE SET NULL,
            material_id INTEGER REFERENCES materials(id) ON DELETE SET NULL,
            display_name VARCHAR(512) NOT NULL DEFAULT '',
            quantity INTEGER NOT NULL DEFAULT 1,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """))
    try:
        conn.execute(text(
            "ALTER TABLE warehouse_defect_records ADD COLUMN printed_stock_id INTEGER REFERENCES printed_part_stock(id) ON DELETE SET NULL"
        ))
    except Exception:
        pass
    try:
        conn.execute(text(
            "ALTER TABLE warehouse_defect_records ADD COLUMN material_id INTEGER REFERENCES materials(id) ON DELETE SET NULL"
        ))
    except Exception:
        pass


def ensure_printed_part_stock_log_table(conn):
    """Журнал изменений остатков напечатанных деталей (склад)."""
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS printed_part_stock_log (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            printed_stock_id INTEGER REFERENCES printed_part_stock(id) ON DELETE SET NULL,
            part_id INTEGER REFERENCES parts(id) ON DELETE SET NULL,
            material_id INTEGER REFERENCES materials(id) ON DELETE SET NULL,
            part_name VARCHAR(256) NOT NULL DEFAULT '',
            material_name VARCHAR(256) NOT NULL DEFAULT '',
            change_kind VARCHAR(16) NOT NULL,
            quantity INTEGER NOT NULL DEFAULT 1
        )
    """))
    try:
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_printed_part_stock_log_created_at ON printed_part_stock_log (created_at)"))
    except Exception:
        pass


def ensure_warehouse_assembly_batch_tables(conn):
    """Партии сборки склада и строки состава (изделие × количество)."""
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS warehouse_assembly_batches (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            status VARCHAR(32) NOT NULL DEFAULT 'created',
            comment VARCHAR(512) NOT NULL DEFAULT '',
            deleted_at TIMESTAMP,
            display_batch_no INTEGER NOT NULL DEFAULT 0
        )
    """))
    try:
        conn.execute(text("ALTER TABLE warehouse_assembly_batches ADD COLUMN deleted_at TIMESTAMP"))
    except Exception:
        pass
    try:
        conn.execute(text(
            "ALTER TABLE warehouse_assembly_batches ADD COLUMN display_batch_no INTEGER NOT NULL DEFAULT 0"
        ))
    except Exception:
        pass
    try:
        conn.execute(text(
            "UPDATE warehouse_assembly_batches SET display_batch_no = id "
            "WHERE display_batch_no IS NULL OR display_batch_no = 0"
        ))
    except Exception:
        pass
    try:
        conn.execute(text(
            "ALTER TABLE warehouse_assembly_batches ADD COLUMN assembled_output_at TIMESTAMP"
        ))
    except Exception:
        pass
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS warehouse_assembly_batch_items (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            batch_id INTEGER NOT NULL REFERENCES warehouse_assembly_batches(id) ON DELETE CASCADE,
            product_id INTEGER NOT NULL REFERENCES products(id) ON DELETE CASCADE,
            quantity INTEGER NOT NULL DEFAULT 1
        )
    """))
    try:
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_wh_assembly_batch_items_batch_id ON warehouse_assembly_batch_items (batch_id)"))
    except Exception:
        pass


def ensure_finance_schema(conn):
    """Таблица направлений финансов и новые поля в finance_entries."""
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS finance_counterparties (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            operation_type VARCHAR(16) NOT NULL,
            name VARCHAR(256) NOT NULL,
            sort_order INTEGER NOT NULL DEFAULT 0
        )
    """))
    try:
        conn.execute(text(
            "CREATE UNIQUE INDEX IF NOT EXISTS uq_finance_counterparties_op_name "
            "ON finance_counterparties(operation_type, name)"
        ))
    except Exception:
        pass
    try:
        conn.execute(text(
            "CREATE INDEX IF NOT EXISTS ix_finance_counterparties_op_sort "
            "ON finance_counterparties(operation_type, sort_order, id)"
        ))
    except Exception:
        pass

    try:
        conn.execute(text(
            "ALTER TABLE finance_entries ADD COLUMN counterparty_id INTEGER "
            "REFERENCES finance_counterparties(id) ON DELETE SET NULL"
        ))
    except Exception:
        pass
    try:
        conn.execute(text(
            "ALTER TABLE finance_entries ADD COLUMN counterparty_name VARCHAR(256) NOT NULL DEFAULT ''"
        ))
    except Exception:
        pass

    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS finance_tags (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            name VARCHAR(128) NOT NULL,
            hex VARCHAR(7) NOT NULL DEFAULT '#607d8b',
            sort_order INTEGER NOT NULL DEFAULT 0,
            applies_to_income INTEGER NOT NULL DEFAULT 1,
            applies_to_expense INTEGER NOT NULL DEFAULT 1
        )
    """))
    try:
        conn.execute(text(
            "CREATE UNIQUE INDEX IF NOT EXISTS uq_finance_tags_name ON finance_tags (name)"
        ))
    except Exception:
        pass
    try:
        conn.execute(text(
            "CREATE INDEX IF NOT EXISTS ix_finance_tags_sort_order ON finance_tags (sort_order, id)"
        ))
    except Exception:
        pass
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS finance_entry_tags (
            finance_entry_id INTEGER NOT NULL REFERENCES finance_entries(id) ON DELETE CASCADE,
            finance_tag_id INTEGER NOT NULL REFERENCES finance_tags(id) ON DELETE CASCADE,
            PRIMARY KEY (finance_entry_id, finance_tag_id)
        )
    """))
    try:
        conn.execute(text(
            "ALTER TABLE finance_tags ADD COLUMN applies_to_income INTEGER NOT NULL DEFAULT 1"
        ))
    except Exception:
        pass
    try:
        conn.execute(text(
            "ALTER TABLE finance_tags ADD COLUMN applies_to_expense INTEGER NOT NULL DEFAULT 1"
        ))
    except Exception:
        pass


def ensure_user_role(conn):
    """Роль пользователя сайта: staff | operator."""
    try:
        conn.execute(text(
            "ALTER TABLE users ADD COLUMN role VARCHAR(32) NOT NULL DEFAULT 'staff'"
        ))
    except Exception:
        pass


def ensure_shift_planning_tables(conn):
    """Листы-задания на смену для операторов."""
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS shift_sheets (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            assignee_user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            shift_date DATE NOT NULL,
            status VARCHAR(32) NOT NULL DEFAULT 'draft',
            manager_notes VARCHAR(1024) NOT NULL DEFAULT '',
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            published_at TIMESTAMP
        )
    """))
    try:
        conn.execute(text(
            "CREATE INDEX IF NOT EXISTS ix_shift_sheets_assignee_date "
            "ON shift_sheets (assignee_user_id, shift_date)"
        ))
    except Exception:
        pass
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS shift_tasks (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            sheet_id INTEGER NOT NULL REFERENCES shift_sheets(id) ON DELETE CASCADE,
            sort_order INTEGER NOT NULL DEFAULT 0,
            task_type VARCHAR(32) NOT NULL DEFAULT 'print',
            title VARCHAR(256) NOT NULL DEFAULT '',
            description TEXT NOT NULL DEFAULT '',
            target_quantity INTEGER NOT NULL DEFAULT 1,
            unit_label VARCHAR(32) NOT NULL DEFAULT 'шт.',
            status VARCHAR(32) NOT NULL DEFAULT 'pending',
            completion_percent INTEGER,
            worker_comment VARCHAR(2000) NOT NULL DEFAULT '',
            completed_at TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """))
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS shift_task_attachments (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            task_id INTEGER NOT NULL REFERENCES shift_tasks(id) ON DELETE CASCADE,
            stored_filename VARCHAR(256) NOT NULL,
            original_filename VARCHAR(256) NOT NULL DEFAULT '',
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """))
    try:
        conn.execute(text(
            "ALTER TABLE shift_tasks ADD COLUMN print_queue_item_id INTEGER "
            "REFERENCES print_queue_items(id) ON DELETE SET NULL"
        ))
    except Exception:
        pass
