"""
Модели базы данных
"""
from sqlalchemy import Column, Integer, String, DateTime, Float, JSON, UniqueConstraint, ForeignKey, Table, Boolean, Date, Text
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from app.db.database import Base


finance_entry_tags = Table(
    "finance_entry_tags",
    Base.metadata,
    Column(
        "finance_entry_id",
        Integer,
        ForeignKey("finance_entries.id", ondelete="CASCADE"),
        primary_key=True,
    ),
    Column(
        "finance_tag_id",
        Integer,
        ForeignKey("finance_tags.id", ondelete="CASCADE"),
        primary_key=True,
    ),
)


class User(Base):
    """Пользователь сайта (логин + пароль для входа на главную)."""
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    username = Column(String(64), unique=True, nullable=False, index=True)
    password_hash = Column(String(256), nullable=False)
    # staff — полный доступ к сайту; operator — листы-задания на смену
    role = Column(String(32), nullable=False, default="staff", index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    shift_sheets = relationship("ShiftSheet", back_populates="assignee", foreign_keys="ShiftSheet.assignee_user_id")


class ShiftSheet(Base):
    """Лист-задание на смену для оператора (печать / сборка / упаковка)."""
    __tablename__ = "shift_sheets"

    id = Column(Integer, primary_key=True, index=True)
    assignee_user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    shift_date = Column(Date, nullable=False, index=True)
    status = Column(String(32), nullable=False, default="draft", index=True)  # draft | published | closed
    manager_notes = Column(String(1024), nullable=False, default="")
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    published_at = Column(DateTime(timezone=True), nullable=True)

    assignee = relationship("User", back_populates="shift_sheets", foreign_keys=[assignee_user_id])
    tasks = relationship(
        "ShiftTask",
        back_populates="sheet",
        cascade="all, delete-orphan",
        order_by="ShiftTask.sort_order",
    )


class ShiftTask(Base):
    """Пункт листа-задания на смену."""
    __tablename__ = "shift_tasks"

    id = Column(Integer, primary_key=True, index=True)
    sheet_id = Column(Integer, ForeignKey("shift_sheets.id", ondelete="CASCADE"), nullable=False, index=True)
    sort_order = Column(Integer, nullable=False, default=0)
    task_type = Column(String(32), nullable=False, default="print")  # print | assemble | pack
    title = Column(String(256), nullable=False, default="")
    description = Column(Text, nullable=False, default="")
    target_quantity = Column(Integer, nullable=False, default=1)
    unit_label = Column(String(32), nullable=False, default="шт.")
    # pending | completed | partial | failed
    status = Column(String(32), nullable=False, default="pending", index=True)
    completion_percent = Column(Integer, nullable=True)
    worker_comment = Column(String(2000), nullable=False, default="")
    completed_at = Column(DateTime(timezone=True), nullable=True)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    sheet = relationship("ShiftSheet", back_populates="tasks")
    attachments = relationship(
        "ShiftTaskAttachment",
        back_populates="task",
        cascade="all, delete-orphan",
    )


class ShiftTaskAttachment(Base):
    """Фото к отчёту оператора по пункту задания."""
    __tablename__ = "shift_task_attachments"

    id = Column(Integer, primary_key=True, index=True)
    task_id = Column(Integer, ForeignKey("shift_tasks.id", ondelete="CASCADE"), nullable=False, index=True)
    stored_filename = Column(String(256), nullable=False)
    original_filename = Column(String(256), nullable=False, default="")
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    task = relationship("ShiftTask", back_populates="attachments")


class Color(Base):
    """Справочник цветов: наименование и hex (для квадратика)."""
    __tablename__ = "colors"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(128), unique=True, nullable=False)
    hex = Column(String(7), nullable=False, default="#000000")


class Material(Base):
    """Справочник материалов (тип пластика, наименование, цвет, вес). Производитель не используется (оставлен для совместимости БД)."""
    __tablename__ = "materials"
    __table_args__ = (UniqueConstraint("name", "color", name="uq_material_name_color"),)

    id = Column(Integer, primary_key=True, index=True)
    plastic_type = Column(String(128), nullable=False, default="")
    name = Column(String(256), nullable=False)
    color = Column(String(128), nullable=False, default="")
    manufacturer = Column(String(256), nullable=False, default="")  # не используется в UI, только для совместимости БД
    weight_grams = Column(Integer, nullable=False, default=1000)


class Spool(Base):
    """Регистр катушек: id, материал, остаток длины (м). QR кодирует идентификатор катушки."""
    __tablename__ = "spools"

    id = Column(Integer, primary_key=True, index=True)
    material_id = Column(Integer, ForeignKey("materials.id", ondelete="SET NULL"), nullable=True)
    remaining_length_m = Column(Float, nullable=False, default=350.0)


class Part(Base):
    """Деталь: наименование, вес, STL-файл, миниатюра STL, фото."""
    __tablename__ = "parts"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(256), nullable=False)
    weight_grams = Column(Integer, nullable=False, default=0)
    stl_filename = Column(String(512), nullable=False, default="")  # имя файла в репозитории (uploads/parts/stl/)
    stl_thumb_filename = Column(String(512), nullable=False, default="")  # миниатюра из STL (uploads/parts/thumbs/)
    photo_filename = Column(String(512), nullable=False, default="")  # имя файла в uploads/parts/photos/


class Product(Base):
    """Изделие: артикул, название, фото; штрихкоды Озон/Вайлдберрис; SKU Ozon; состав — ProductPart; варианты индивидуальной упаковки — ProductIndividualPackaging."""
    __tablename__ = "products"

    id = Column(Integer, primary_key=True, index=True)
    article = Column(String(128), nullable=False, default="")
    name = Column(String(256), nullable=False)
    photo_filename = Column(String(512), nullable=False, default="")
    ozon_barcode_filename = Column(String(512), nullable=False, default="")
    wildberries_barcode_filename = Column(String(512), nullable=False, default="")
    ozon_sku = Column(Integer, nullable=True)  # SKU (product_id) в каталоге Ozon для заявок на поставку
    wildberries_sku = Column(Integer, nullable=True)  # SKU (nm_id) в каталоге Wildberries


class ProductPart(Base):
    """Состав изделия: изделие — детали с количеством и материалом."""
    __tablename__ = "product_parts"

    id = Column(Integer, primary_key=True, index=True)
    product_id = Column(Integer, ForeignKey("products.id", ondelete="CASCADE"), nullable=False)
    part_id = Column(Integer, ForeignKey("parts.id", ondelete="CASCADE"), nullable=False)
    material_id = Column(Integer, ForeignKey("materials.id", ondelete="SET NULL"), nullable=True)
    quantity = Column(Integer, nullable=False, default=1)


class ExtraMaterial(Base):
    """Дополнительный материал: наименование и фото (проволока, клей и т.п.)."""
    __tablename__ = "extra_materials"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(256), nullable=False)
    photo_filename = Column(String(512), nullable=False, default="")


class ProductExtraMaterial(Base):
    """Связь изделия с дополнительными материалами (количество)."""
    __tablename__ = "product_extra_materials"

    id = Column(Integer, primary_key=True, index=True)
    product_id = Column(Integer, ForeignKey("products.id", ondelete="CASCADE"), nullable=False)
    extra_material_id = Column(Integer, ForeignKey("extra_materials.id", ondelete="CASCADE"), nullable=False)
    quantity = Column(Integer, nullable=False, default=1)


class WarehouseExtraStock(Base):
    """Оприходованные на складе дополнительные материалы (из справочника Дополнительные материалы)."""
    __tablename__ = "warehouse_extra_stock"

    id = Column(Integer, primary_key=True, index=True)
    extra_material_id = Column(
        Integer, ForeignKey("extra_materials.id", ondelete="CASCADE"), nullable=False, unique=True
    )
    quantity = Column(Integer, nullable=False, default=0)


class PrintedPartStock(Base):
    """Остатки напечатанных деталей на складе (еще не собраны в изделия)."""
    __tablename__ = "printed_part_stock"

    id = Column(Integer, primary_key=True, index=True)
    part_id = Column(Integer, ForeignKey("parts.id", ondelete="CASCADE"), nullable=False)
    material_id = Column(Integer, ForeignKey("materials.id", ondelete="SET NULL"), nullable=True)
    quantity = Column(Integer, nullable=False, default=0)


class PrintedPartStockLog(Base):
    """Журнал изменений остатков напечатанных деталей (добавление, изъятие, брак, резерв под сборку). Хранение 3 мес. на стороне приложения."""

    __tablename__ = "printed_part_stock_log"

    id = Column(Integer, primary_key=True, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False, index=True)
    printed_stock_id = Column(Integer, ForeignKey("printed_part_stock.id", ondelete="SET NULL"), nullable=True)
    part_id = Column(Integer, ForeignKey("parts.id", ondelete="SET NULL"), nullable=True)
    material_id = Column(Integer, ForeignKey("materials.id", ondelete="SET NULL"), nullable=True)
    part_name = Column(String(256), nullable=False, default="")
    material_name = Column(String(256), nullable=False, default="")
    change_kind = Column(String(16), nullable=False)  # add | remove | defect | defect_return | assembly | assembly_return
    quantity = Column(Integer, nullable=False, default=1)


class WarehouseAssemblyBatch(Base):
    """Партия сборки на складе: изделия и количества; детали зарезервированы (списаны из напечатанных)."""

    __tablename__ = "warehouse_assembly_batches"

    id = Column(Integer, primary_key=True, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    status = Column(String(32), nullable=False, default="created")  # created | in_progress | completed
    comment = Column(String(512), nullable=False, default="")
    deleted_at = Column(DateTime(timezone=True), nullable=True)
    # Порядковый номер для UI и отчётов; следующий = max(...) + 1 по всем строкам, включая удалённые.
    display_batch_no = Column(Integer, nullable=False, default=0, index=True)
    # Один раз при первом переводе в «Выполнена»: оприходование в собранные изделия.
    assembled_output_at = Column(DateTime(timezone=True), nullable=True)


class WarehouseAssemblyBatchItem(Base):
    """Строка состава партии сборки: изделие и количество."""

    __tablename__ = "warehouse_assembly_batch_items"

    id = Column(Integer, primary_key=True, index=True)
    batch_id = Column(Integer, ForeignKey("warehouse_assembly_batches.id", ondelete="CASCADE"), nullable=False, index=True)
    product_id = Column(Integer, ForeignKey("products.id", ondelete="CASCADE"), nullable=False)
    quantity = Column(Integer, nullable=False, default=1)


class AssembledProductStock(Base):
    """Остатки собранных изделий на складе."""
    __tablename__ = "assembled_product_stock"

    id = Column(Integer, primary_key=True, index=True)
    product_id = Column(Integer, ForeignKey("products.id", ondelete="CASCADE"), nullable=False, unique=True)
    quantity = Column(Integer, nullable=False, default=0)


class AssembledProductStockLog(Base):
    """Журнал движений собранных изделий (вкладка «Собранные изделия»)."""

    __tablename__ = "assembled_product_stock_log"

    id = Column(Integer, primary_key=True, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False, index=True)
    product_id = Column(Integer, ForeignKey("products.id", ondelete="SET NULL"), nullable=True, index=True)
    product_label = Column(String(512), nullable=False, default="")
    delta_qty = Column(Integer, nullable=False)
    action_kind = Column(String(32), nullable=False)
    assembly_batch_id = Column(Integer, ForeignKey("warehouse_assembly_batches.id", ondelete="SET NULL"), nullable=True)


class WrittenOffMaterial(Base):
    """Списанные материалы: катушки (закончен филамент) и доп. материалы (использованы в изделиях)."""
    __tablename__ = "written_off_materials"

    id = Column(Integer, primary_key=True, index=True)
    item_type = Column(String(16), nullable=False)  # 'spool' | 'extra'
    spool_id = Column(Integer, ForeignKey("spools.id", ondelete="SET NULL"), nullable=True)
    material_id = Column(Integer, ForeignKey("materials.id", ondelete="SET NULL"), nullable=True)  # для катушки при возврате
    extra_material_id = Column(Integer, ForeignKey("extra_materials.id", ondelete="SET NULL"), nullable=True)
    display_name = Column(String(512), nullable=False, default="")
    quantity = Column(Integer, nullable=False, default=1)
    written_off_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class WarehouseDefectRecord(Base):
    """Журнал брака по деталям и изделиям."""
    __tablename__ = "warehouse_defect_records"

    id = Column(Integer, primary_key=True, index=True)
    item_type = Column(String(16), nullable=False)  # 'part' | 'product'
    printed_stock_id = Column(Integer, ForeignKey("printed_part_stock.id", ondelete="SET NULL"), nullable=True)
    part_id = Column(Integer, ForeignKey("parts.id", ondelete="SET NULL"), nullable=True)
    product_id = Column(Integer, ForeignKey("products.id", ondelete="SET NULL"), nullable=True)
    material_id = Column(Integer, ForeignKey("materials.id", ondelete="SET NULL"), nullable=True)
    display_name = Column(String(512), nullable=False, default="")
    quantity = Column(Integer, nullable=False, default=1)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class IndividualPackaging(Base):
    """Индивидуальная упаковка: наименование, фото, размеры ДхШхВ (мм). Объём вычисляется: (Д×Ш×В)/1_000_000 л."""
    __tablename__ = "individual_packaging"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(256), nullable=False)
    photo_filename = Column(String(512), nullable=False, default="")
    length_mm = Column(Integer, nullable=False, default=0)
    width_mm = Column(Integer, nullable=False, default=0)
    height_mm = Column(Integer, nullable=False, default=0)


class TransportPackaging(Base):
    """Транспортировочная упаковка: наименование, фото, размеры ДхШхВ (мм). Объём вычисляется: (Д×Ш×В)/1_000_000 л."""
    __tablename__ = "transport_packaging"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(256), nullable=False)
    photo_filename = Column(String(512), nullable=False, default="")
    length_mm = Column(Integer, nullable=False, default=0)
    width_mm = Column(Integer, nullable=False, default=0)
    height_mm = Column(Integer, nullable=False, default=0)


class ProductIndividualPackaging(Base):
    """Связь изделия с индивидуальной упаковкой и количеством (варианты упаковки изделия)."""
    __tablename__ = "product_individual_packaging"

    id = Column(Integer, primary_key=True, index=True)
    product_id = Column(Integer, ForeignKey("products.id", ondelete="CASCADE"), nullable=False)
    individual_packaging_id = Column(Integer, ForeignKey("individual_packaging.id", ondelete="CASCADE"), nullable=False)
    quantity = Column(Integer, nullable=False, default=1)


class AssemblyOption(Base):
    """Сборочный вариант: наименование сборки, одна трансп. упаковка, несколько инд. упаковок с количеством."""
    __tablename__ = "assembly_options"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(256), nullable=False)
    transport_packaging_id = Column(Integer, ForeignKey("transport_packaging.id", ondelete="SET NULL"), nullable=True)


class AssemblyOptionItem(Base):
    """Состав сборочного варианта: индивидуальная упаковка и количество."""
    __tablename__ = "assembly_option_items"

    id = Column(Integer, primary_key=True, index=True)
    assembly_option_id = Column(Integer, ForeignKey("assembly_options.id", ondelete="CASCADE"), nullable=False)
    individual_packaging_id = Column(Integer, ForeignKey("individual_packaging.id", ondelete="CASCADE"), nullable=False)
    quantity = Column(Integer, nullable=False, default=1)


class Printer(Base):
    """Принтер: наименование, номер, размер печатного стола, IP-адрес, текущая катушка (филамент)."""
    __tablename__ = "printers"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(256), nullable=False, default="")
    number = Column(String(64), nullable=False, default="")
    bed_size = Column(String(128), nullable=False, default="")
    ip_address = Column(String(64), nullable=False, default="")
    current_spool_id = Column(Integer, ForeignKey("spools.id", ondelete="SET NULL"), nullable=True)


class PrintJob(Base):
    """Задание на печать: название, детали с кол-вом, принтеры, время, вес материала, gcode."""
    __tablename__ = "print_jobs"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(256), nullable=False, default="")
    part_quantities = Column(JSON, nullable=False, default=list)  # [{"part_id": int, "qty": int}, ...]
    printer_ids = Column(JSON, nullable=False, default=list)   # [int, ...]
    execution_time = Column(String(128), nullable=False, default="")   # например "2 ч 30 мин"
    material_weight_grams = Column(Float, nullable=False, default=0.0)
    gcode_filename = Column(String(512), nullable=False, default="")
    gcode_thumb_filename = Column(String(512), nullable=False, default="")


class PrintQueueItem(Base):
    """Элемент очереди печати: задание, принтер, материал, время начала (окончание = начало + длительность задания)."""
    __tablename__ = "print_queue_items"

    id = Column(Integer, primary_key=True, index=True)
    sequence = Column(Integer, nullable=False, default=0)  # порядковый номер задания в очереди
    print_job_id = Column(Integer, ForeignKey("print_jobs.id", ondelete="CASCADE"), nullable=False)
    printer_id = Column(Integer, ForeignKey("printers.id", ondelete="CASCADE"), nullable=False)
    material_id = Column(Integer, ForeignKey("materials.id", ondelete="SET NULL"), nullable=True)
    scheduled_start = Column(DateTime(timezone=True), nullable=False)


class PrintPlan(Base):
    """План печати на неделю: дата понедельника (week_start), хранение 6 месяцев."""
    __tablename__ = "print_plans"

    id = Column(Integer, primary_key=True, index=True)
    week_start = Column(String(10), nullable=False, index=True)  # YYYY-MM-DD (понедельник)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class PrintPlanItem(Base):
    """Позиция плана печати: изделие и количество."""
    __tablename__ = "print_plan_items"

    id = Column(Integer, primary_key=True, index=True)
    print_plan_id = Column(Integer, ForeignKey("print_plans.id", ondelete="CASCADE"), nullable=False)
    product_id = Column(Integer, ForeignKey("products.id", ondelete="CASCADE"), nullable=False)
    quantity = Column(Integer, nullable=False, default=1)


class Order(Base):
    """Модель заказа"""
    __tablename__ = "orders"

    id = Column(Integer, primary_key=True, index=True)
    marketplace = Column(String, nullable=False, index=True)  # 'ozon' или 'wildberries'
    order_id = Column(String, unique=True, nullable=False, index=True)  # ID заказа на маркетплейсе
    status = Column(String, nullable=False)
    amount = Column(Float, nullable=False)
    currency = Column(String, default="RUB")
    created_at = Column(DateTime(timezone=True), nullable=False)
    updated_at = Column(DateTime(timezone=True), nullable=False)
    raw_data = Column(JSON)  # Полные данные от API
    created_db_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_db_at = Column(DateTime(timezone=True), onupdate=func.now())


class PackagingTask(Base):
    """Упаковочное задание: номер, маркетплейс, номер поставки, статус. Состав — PackagingTaskItem."""
    __tablename__ = "packaging_tasks"

    id = Column(Integer, primary_key=True, index=True)
    number = Column(String(64), nullable=False, default="")
    marketplace = Column(String(64), nullable=False, default="")
    delivery_number = Column(String(128), nullable=False, default="")
    status = Column(String(32), nullable=False, default="created")  # created, assembled, sent, cancelled


class PackagingTaskItem(Base):
    """Состав упаковочного задания: изделие и количество."""
    __tablename__ = "packaging_task_items"

    id = Column(Integer, primary_key=True, index=True)
    packaging_task_id = Column(Integer, ForeignKey("packaging_tasks.id", ondelete="CASCADE"), nullable=False)
    product_id = Column(Integer, ForeignKey("products.id", ondelete="CASCADE"), nullable=False)
    quantity = Column(Integer, nullable=False, default=1)


class SupplyDraftConfig(Base):
    """Конфиг черновика для фонового парсера очереди поставок (один ряд, body для POST /v1/draft/crossdock/create)."""
    __tablename__ = "supply_draft_config"

    id = Column(Integer, primary_key=True, index=True)
    draft_body = Column(JSON, nullable=False)  # тело запроса; при скане подставляется macrolocal_cluster_id
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class SlotsTrackerConfig(Base):
    """Конфиг «Отслеживателя»: отслеживание свободных таймслотов по выбранным кластерам с уведомлением в Telegram."""
    __tablename__ = "slots_tracker_config"

    id = Column(Integer, primary_key=True, index=True)
    cluster_ids = Column(JSON, nullable=False, default=list)  # [int, ...] — macrolocal_cluster_id кластеров
    period_days = Column(Integer, nullable=False, default=7)   # 7, 14 или 21 — период поиска слотов (дней)
    items = Column(JSON, nullable=False, default=list)       # [{"sku": int, "quantity": int}, ...] — состав (по 50 шт каждого SKU)
    frequency_hours = Column(Integer, nullable=False, default=4)  # частота парсинга (часов)
    enabled = Column(Integer, nullable=False, default=1)     # 1 — включён, 0 — выключен
    last_run_at = Column(DateTime(timezone=True), nullable=True)  # последний запуск (для интервала)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class SupplyQueueScan(Base):
    """Один запуск сканирования очереди поставок (дата/время)."""
    __tablename__ = "supply_queue_scans"

    id = Column(Integer, primary_key=True, index=True)
    scanned_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class SupplyQueueResult(Base):
    """Результат по одному кластеру за один скан: склад (кластер) и доступные даты."""
    __tablename__ = "supply_queue_results"

    id = Column(Integer, primary_key=True, index=True)
    scan_id = Column(Integer, ForeignKey("supply_queue_scans.id", ondelete="CASCADE"), nullable=False, index=True)
    cluster_id = Column(Integer, nullable=False)
    cluster_name = Column(String(256), nullable=False, default="")
    dates_text = Column(String(512), nullable=False, default="")  # "18.03, 19.03" или "нет дат"
    # 7 чисел по дням недели: -1 = ошибка, 0 = нет слотов, 1 = красный, 2–6 = жёлтый, 7+ = зелёный
    day_counts = Column(JSON, nullable=True)


class OzonSupply(Base):
    """Заявка на поставку Ozon (созданная через черновик + таймслот)."""
    __tablename__ = "ozon_supplies"

    id = Column(Integer, primary_key=True, index=True)
    ozon_supply_id = Column(String(64), nullable=False, default="", index=True)  # order_id (8 цифр) для API отмены
    posting_number = Column(String(32), nullable=False, default="")  # 13-значный номер поставки (из v3/supply-order/get)
    draft_id = Column(String(32), nullable=True, default=None)  # ID черновика в ЛК Ozon для ссылки seller.ozon.ru/app/supply/draft/{draft_id}
    crossdock_cluster_id = Column(Integer, nullable=True, default=None)  # macrolocal_cluster_id выбранного кластера (для подсветки в таблице кластеров)
    destination_warehouse = Column(String(256), nullable=False, default="")  # направление: склад, на который приедет поставка
    shipment_date = Column(String(32), nullable=False, default="")   # дата отправки (из слота или пусто)
    timeslot_from = Column(String(64), nullable=True, default=None)  # начало таймслота (ISO), для отображения "с 11:00"
    timeslot_to = Column(String(64), nullable=True, default=None)    # конец таймслота (ISO), для отображения "до 12:00"
    delivery_date_estimated = Column(String(32), nullable=False, default="")  # предполагаемая дата доставки
    composition = Column(JSON, nullable=False, default=list)  # [{"product_id": int, "sku": int, "quantity": int, "product_name": str}, ...]
    status = Column(String(64), nullable=False, default="created")
    has_cargo_places = Column(Integer, nullable=False, default=0)  # 1 — грузоместа успешно загружены (кнопка «Редактировать грузоместа»)
    cargo_places_status = Column(String(32), nullable=False, default="")  # PENDING, IN_PROGRESS, SUCCESS, FAILED — статус записи грузомест (обновляется в фоне)
    cargo_places_data = Column(JSON, nullable=False, default=list)  # [{"cargo_id": int?, "key": str, "type": "BOX"|"PALLET", "items": [{"sku": int, "quantity": int}]}]
    status_check_error = Column(String(512), nullable=True)  # ошибка при опросе статуса поставки (v3/supply-order/get)
    composition_mismatch_message = Column(String(512), nullable=True)  # предупреждение о расхождении заявленного состава с составом в Озон
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class FinanceEntry(Base):
    """Лог финансовых операций: доходы и расходы."""
    __tablename__ = "finance_entries"

    id = Column(Integer, primary_key=True, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False, index=True)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)
    operation_type = Column(String(16), nullable=False, index=True)  # income | expense
    counterparty_id = Column(Integer, ForeignKey("finance_counterparties.id", ondelete="SET NULL"), nullable=True, index=True)
    counterparty_name = Column(String(256), nullable=False, default="")
    comment = Column(String(512), nullable=False, default="")
    amount = Column(Float, nullable=False, default=0.0)
    tags = relationship(
        "FinanceTag",
        secondary=finance_entry_tags,
        back_populates="entries",
        lazy="selectin",
    )


class FinanceCounterparty(Base):
    """Справочник направлений операций: income (Откуда), expense (Куда)."""
    __tablename__ = "finance_counterparties"
    __table_args__ = (
        UniqueConstraint("operation_type", "name", name="uq_finance_counterparties_op_name"),
    )

    id = Column(Integer, primary_key=True, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    operation_type = Column(String(16), nullable=False, index=True)  # income | expense
    name = Column(String(256), nullable=False)
    sort_order = Column(Integer, nullable=False, default=0, index=True)


class FinanceTag(Base):
    """Тег для финансовых операций (справочник): название и цвет."""
    __tablename__ = "finance_tags"
    __table_args__ = (UniqueConstraint("name", name="uq_finance_tags_name"),)

    id = Column(Integer, primary_key=True, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    name = Column(String(128), nullable=False)
    hex = Column(String(7), nullable=False, default="#607d8b")
    sort_order = Column(Integer, nullable=False, default=0, index=True)
    applies_to_income = Column(Boolean, nullable=False, default=True)
    applies_to_expense = Column(Boolean, nullable=False, default=True)

    entries = relationship(
        "FinanceEntry",
        secondary=finance_entry_tags,
        back_populates="tags",
    )
