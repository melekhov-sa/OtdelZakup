"""ORM models for the application."""

import json
from datetime import datetime, timezone

from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Integer, String, Text, UniqueConstraint

from app.database import Base


class NomenclatureFolder(Base):
    """Folder (group) from 1C Nomenclature directory."""

    __tablename__ = "nomenclature_folder"

    folder_uid  = Column(String(100), primary_key=True)
    folder_name = Column(String(300), nullable=False, default="")
    parent_uid  = Column(String(100), nullable=True)
    folder_path = Column(String(500), nullable=False, default="")
    priority    = Column(Integer, nullable=True)   # 1 = highest; None = no preference
    updated_at  = Column(DateTime, nullable=True,
                         default=lambda: datetime.now(timezone.utc),
                         onupdate=lambda: datetime.now(timezone.utc))


class ReadinessRule(Base):
    __tablename__ = "readiness_rule"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(200), nullable=False)
    description = Column(Text, nullable=False, default="")
    item_type = Column(String(50), nullable=True)
    require_fields = Column(Text, nullable=False, default="[]")
    priority = Column(Integer, nullable=False, default=0)
    is_active = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime, nullable=False, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime, nullable=False, default=lambda: datetime.now(timezone.utc),
                        onupdate=lambda: datetime.now(timezone.utc))

    @property
    def require_fields_list(self) -> list[str]:
        if not self.require_fields:
            return []
        val = self.require_fields
        if isinstance(val, list):
            return val
        return json.loads(val)

    @require_fields_list.setter
    def require_fields_list(self, value: list[str]):
        self.require_fields = json.dumps(value, ensure_ascii=False)


class ValidationRule(Base):
    __tablename__ = "validation_rule"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(200), nullable=False)
    description = Column(Text, nullable=False, default="")
    item_type = Column(String(50), nullable=True)
    require_fields = Column(Text, nullable=False, default="[]")
    forbid_fields = Column(Text, nullable=False, default="[]")
    force_status = Column(String(20), nullable=True)  # "review" | "manual" | None
    # condition_type determines validation logic:
    #   FIELDS_REQUIRED – require_fields must be filled (default, backwards-compat)
    #   FIELDS_FORBIDDEN – forbid_fields must be empty
    #   STANDARD_MATCH  – item_type must match what the standard directory says
    condition_type = Column(String(20), nullable=False, default="FIELDS_REQUIRED")
    # STANDARD_MATCH parameters:
    standard_source = Column(String(10), nullable=False, default="ANY")  # ANY|DIN|ISO|GOST
    expected_item_type_mode = Column(String(20), nullable=False, default="FROM_DIRECTORY")  # FROM_DIRECTORY|FIXED
    expected_item_type = Column(String(50), nullable=True)
    priority = Column(Integer, nullable=False, default=0)
    is_active = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime, nullable=False, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime, nullable=False, default=lambda: datetime.now(timezone.utc),
                        onupdate=lambda: datetime.now(timezone.utc))

    @property
    def require_fields_list(self) -> list[str]:
        if not self.require_fields:
            return []
        val = self.require_fields
        if isinstance(val, list):
            return val
        return json.loads(val)

    @require_fields_list.setter
    def require_fields_list(self, value: list[str]):
        self.require_fields = json.dumps(value, ensure_ascii=False)

    @property
    def forbid_fields_list(self) -> list[str]:
        if not self.forbid_fields:
            return []
        val = self.forbid_fields
        if isinstance(val, list):
            return val
        return json.loads(val)

    @forbid_fields_list.setter
    def forbid_fields_list(self, value: list[str]):
        self.forbid_fields = json.dumps(value, ensure_ascii=False)

    @property
    def force_status_label(self) -> str:
        labels = {"review": "Требуется просмотреть", "manual": "Требуется вручную разобрать"}
        return labels.get(self.force_status or "", "—")

    @property
    def condition_type_label(self) -> str:
        labels = {
            "FIELDS_REQUIRED": "Обязательные поля",
            "FIELDS_FORBIDDEN": "Запрещённые поля",
            "STANDARD_MATCH": "Тип ↔ Стандарт",
        }
        return labels.get(self.condition_type or "FIELDS_REQUIRED", self.condition_type or "—")

    @property
    def standard_source_label(self) -> str:
        labels = {"ANY": "Любой", "DIN": "DIN", "ISO": "ISO", "GOST": "ГОСТ"}
        return labels.get(self.standard_source or "ANY", self.standard_source or "—")


class StandardRef(Base):
    __tablename__ = "standard_ref"
    __table_args__ = (UniqueConstraint("standard_kind", "standard_code", name="uq_standard_ref_kind_code"),)

    id = Column(Integer, primary_key=True, autoincrement=True)
    standard_kind = Column(String(10), nullable=False)   # "GOST" | "ISO" | "DIN"
    standard_code = Column(String(100), nullable=False)  # "7798-70" / "4017" / "931"
    standard_key = Column(String(120), nullable=True, index=True)  # "GOST-7798-70" canonical key
    aliases_json = Column(Text, nullable=True)           # JSON list of alternate spellings
    title = Column(String(300), nullable=True)
    item_type = Column(String(50), nullable=True)
    notes = Column(Text, nullable=True)
    is_active = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime, nullable=False, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime, nullable=False, default=lambda: datetime.now(timezone.utc),
                        onupdate=lambda: datetime.now(timezone.utc))

    @property
    def full_standard(self) -> str:
        """Return full standard string like 'DIN 931', 'ISO 4017', 'ГОСТ 7798-70'."""
        prefix = {"GOST": "ГОСТ", "ISO": "ISO", "DIN": "DIN"}.get(self.standard_kind, self.standard_kind)
        return f"{prefix} {self.standard_code}"


class InferenceRule(Base):
    """Rule for computing missing fields from other extracted data.

    target_field: "size" (currently the only supported target)
    mode values:
      DIAMETER_AS_SIZE          – size = diameter  (e.g. nut M20 → size "M20")
      DIAMETER_X_LENGTH_AS_SIZE – size = diameter + "x" + length  (e.g. bolt M12+80 → "M12x80")
    conditions_json: JSON object; default {"only_if_empty": true}
    """

    __tablename__ = "inference_rule"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(200), nullable=False)
    is_active = Column(Boolean, nullable=False, default=True)
    target_field = Column(String(50), nullable=False, default="size")
    item_types = Column(Text, nullable=True)   # JSON list; NULL = applies to all item types
    mode = Column(String(30), nullable=False, default="DIAMETER_AS_SIZE")
    conditions_json = Column(Text, nullable=True)  # JSON; None = default conditions
    priority = Column(Integer, nullable=False, default=0)
    created_at = Column(DateTime, nullable=False, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime, nullable=False,
                        default=lambda: datetime.now(timezone.utc),
                        onupdate=lambda: datetime.now(timezone.utc))

    @property
    def item_types_list(self) -> list[str]:
        if not self.item_types:
            return []
        val = self.item_types
        if isinstance(val, list):
            return val
        return json.loads(val)

    @item_types_list.setter
    def item_types_list(self, value: list[str]):
        self.item_types = json.dumps(value, ensure_ascii=False) if value else None

    @property
    def mode_label(self) -> str:
        labels = {
            "DIAMETER_AS_SIZE": "Размер = Диаметр",
            "DIAMETER_X_LENGTH_AS_SIZE": "Размер = Диаметр × Длина",
        }
        return labels.get(self.mode, self.mode)



class InternalItem(Base):
    """Our internal catalog item (Наша номенклатура)."""

    __tablename__ = "internal_item"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(500), nullable=False)
    item_type = Column(String(50), nullable=True)
    size = Column(String(50), nullable=True)
    diameter = Column(String(30), nullable=True)
    length = Column(String(30), nullable=True)
    standard_text = Column(String(100), nullable=True)
    strength_class = Column(String(30), nullable=True)
    material_coating = Column(String(100), nullable=True)
    is_active = Column(Boolean, nullable=False, default=True)
    name_full = Column(String(500), nullable=True)        # full name as entered by user
    parse_status = Column(String(10), nullable=True)      # ok / review / manual
    parse_reason = Column(String(300), nullable=True)     # explanation when not ok
    standard_key = Column(String(120), nullable=True, index=True)  # "DIN-438" canonical key
    canonical_key = Column(String(500), nullable=True, index=True)  # dedup key: "type=болт|std=GOST-7798-70|size=12x60"
    size_norm = Column(String(100), nullable=True, index=True)       # normalized size: "M24X50"
    # 1C sync fields
    uid_1c          = Column(String(100), nullable=True, index=True)   # GUID номенклатуры из 1С
    uid_1c_char     = Column(String(100), nullable=True)               # GUID характеристики (null если нет)
    folder_uid      = Column(String(100), nullable=True)               # FK → NomenclatureFolder
    folder_name     = Column(String(300), nullable=True)               # денормализовано для отображения
    folder_path     = Column(String(500), nullable=True)               # полный путь папки
    folder_priority = Column(Integer,     nullable=True)               # приоритет папки (денормализован)
    created_at = Column(DateTime, nullable=False, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime, nullable=False, default=lambda: datetime.now(timezone.utc),
                        onupdate=lambda: datetime.now(timezone.utc))


class SupplierInternalMatch(Base):
    """Memory: fingerprint -> internal_item_id."""

    __tablename__ = "supplier_internal_match"
    __table_args__ = (UniqueConstraint("fingerprint", name="uq_sim_fingerprint"),)

    id = Column(Integer, primary_key=True, autoincrement=True)
    fingerprint = Column(String(64), nullable=False)
    internal_item_id = Column(Integer, nullable=False)
    created_at = Column(DateTime, nullable=False, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime, nullable=False, default=lambda: datetime.now(timezone.utc),
                        onupdate=lambda: datetime.now(timezone.utc))

class SandboxSession(Base):
    """Snapshot of all rules at a point in time, for safe rule experimentation."""

    __tablename__ = "sandbox_session"

    id = Column(Integer, primary_key=True, autoincrement=True)
    created_at = Column(DateTime, nullable=False, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime, nullable=False, default=lambda: datetime.now(timezone.utc),
                        onupdate=lambda: datetime.now(timezone.utc))
    base_version = Column(String(200), nullable=True)   # human-readable label
    rule_snapshot_json = Column(Text, nullable=False)   # JSON blob with all rule lists
    is_active = Column(Boolean, nullable=False, default=True)
    is_applied = Column(Boolean, nullable=False, default=False)
    last_file_id = Column(String(64), nullable=True)    # file_id of last sandbox-processed file


class RuleVersion(Base):
    """Immutable history record created when a sandbox is applied to prod."""

    __tablename__ = "rule_version"

    id = Column(Integer, primary_key=True, autoincrement=True)
    created_at = Column(DateTime, nullable=False, default=lambda: datetime.now(timezone.utc))
    description = Column(String(500), nullable=True)
    snapshot_json = Column(Text, nullable=False)


class SystemSetting(Base):
    """Key-value store for system-wide settings (e.g. auto-match thresholds)."""

    __tablename__ = "system_setting"

    key = Column(String(100), primary_key=True)
    value = Column(Text, nullable=True)
    updated_at = Column(DateTime, nullable=False, default=lambda: datetime.now(timezone.utc),
                        onupdate=lambda: datetime.now(timezone.utc))


class TailPhrase(Base):
    """System stop-phrases stripped from the end of item names before field extraction."""

    __tablename__ = "system_tail_phrase"

    id = Column(Integer, primary_key=True, autoincrement=True)
    phrase = Column(String(500), nullable=False, unique=True)
    is_active = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime, nullable=False, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime, nullable=False, default=lambda: datetime.now(timezone.utc),
                        onupdate=lambda: datetime.now(timezone.utc))


class NameTemplate(Base):
    __tablename__ = "name_template"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(200), nullable=False)
    template_string = Column(String(500), nullable=False)
    is_active = Column(Boolean, nullable=False, default=True)
    priority = Column(Integer, nullable=False, default=1)
    created_at = Column(DateTime, nullable=False, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime, nullable=False, default=lambda: datetime.now(timezone.utc),
                        onupdate=lambda: datetime.now(timezone.utc))


class StandardEquivalent(Base):
    """Bidirectional equivalence between two canonical standards.

    e.g. GOST-7798-70 ↔ DIN-933 (functionally equivalent, different origin)
    """

    __tablename__ = "standard_equivalents"
    __table_args__ = (UniqueConstraint("src_canonical", "dst_canonical", name="uq_std_equiv_pair"),)

    id = Column(Integer, primary_key=True, autoincrement=True)
    src_canonical = Column(String(120), nullable=False, index=True)
    dst_canonical = Column(String(120), nullable=False, index=True)
    confidence = Column(Integer, nullable=False, default=100)  # 0..100
    is_active = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime, nullable=False, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime, nullable=False, default=lambda: datetime.now(timezone.utc),
                        onupdate=lambda: datetime.now(timezone.utc))


class MasterItem(Base):
    """Logical grouping of internal catalog items (Группа объединения)."""

    __tablename__ = "master_items"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(Text, nullable=False)
    description = Column(Text, nullable=True)
    is_active = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime, nullable=False, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime, nullable=False, default=lambda: datetime.now(timezone.utc),
                        onupdate=lambda: datetime.now(timezone.utc))


class MasterItemMember(Base):
    """Membership of an InternalItem (identified by 1C GUID) in a MasterItem group."""

    __tablename__ = "master_item_members"
    __table_args__ = (UniqueConstraint("master_item_id", "onec_guid", name="uq_master_member_pair"),)

    id = Column(Integer, primary_key=True, autoincrement=True)
    master_item_id = Column(Integer, nullable=False)        # FK -> master_items.id
    onec_guid = Column(Text, nullable=False)                # uid_1c from InternalItem
    name_original = Column(Text, nullable=True)             # denormalized name at time of addition
    is_primary = Column(Boolean, nullable=False, default=False)
    created_at = Column(DateTime, nullable=False, default=lambda: datetime.now(timezone.utc))


class ImportAttachment(Base):
    """A raw file uploaded for PDF/image import (before parsing)."""

    __tablename__ = "import_attachment"

    id           = Column(Integer, primary_key=True, autoincrement=True)
    file_id      = Column(String(64), nullable=False, index=True)
    filename     = Column(String(300), nullable=False, default="")
    mime_type    = Column(String(100), nullable=False, default="")
    storage_path = Column(String(500), nullable=False, default="")
    kind         = Column(String(20), nullable=False, default="")  # TEXT_PDF | SCAN_PDF | IMAGE | UNKNOWN
    created_at   = Column(DateTime, nullable=False, default=lambda: datetime.now(timezone.utc))


class ImportParseAttempt(Base):
    """Result of one parsing attempt for an ImportAttachment."""

    __tablename__ = "import_parse_attempt"

    id            = Column(Integer, primary_key=True, autoincrement=True)
    file_id       = Column(String(64), nullable=False, index=True)
    attachment_id = Column(Integer, nullable=True)   # FK -> import_attachment.id
    method        = Column(String(30), nullable=False, default="")  # pdfplumber | ocr_tesseract
    status        = Column(String(20), nullable=False, default="")  # ok | error | empty
    rows_found    = Column(Integer, nullable=False, default=0)
    metrics_json  = Column(Text, nullable=False, default="{}")
    error_text    = Column(Text, nullable=True)
    created_at    = Column(DateTime, nullable=False, default=lambda: datetime.now(timezone.utc))

    @property
    def metrics(self) -> dict:
        try:
            return json.loads(self.metrics_json or "{}")
        except (ValueError, TypeError):
            return {}

    @metrics.setter
    def metrics(self, value: dict) -> None:
        self.metrics_json = json.dumps(value, ensure_ascii=False)


class ProductType(Base):
    """Managed directory of product types with aliases for matching."""
    __tablename__ = "product_type"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), nullable=False, unique=True)
    aliases_json = Column(Text, nullable=False, default="[]")
    is_active = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime, nullable=False, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime, nullable=False, default=lambda: datetime.now(timezone.utc),
                        onupdate=lambda: datetime.now(timezone.utc))

    @property
    def aliases(self) -> list[str]:
        try:
            return json.loads(self.aliases_json or "[]")
        except (ValueError, TypeError):
            return []

    @aliases.setter
    def aliases(self, value: list[str]) -> None:
        self.aliases_json = json.dumps(value, ensure_ascii=False)


# ── Category-based validation rule engine ─────────────────────────────────────

# Canonical field dictionary for required_fields references
VALIDATION_FIELD_LABELS = {
    "type": "Тип изделия",
    "name": "Наименование",
    "standard": "Стандарт",
    "execution_type": "Тип исполнения",
    "material": "Материал",
    "steel_grade": "Марка стали",
    "coating": "Покрытие",
    "strength_class": "Класс прочности",
    "diameter": "Диаметр",
    "length": "Длина",
    "width": "Ширина",
    "thickness": "Толщина",
    "size": "Размер",
    "load_capacity": "Грузоподъемность",
    "shape": "Форма",
    "flange_type": "Тип фланца",
}


class BaseValidationRule(Base):
    """Category-based validation rule with required fields per product group."""

    __tablename__ = "base_validation_rule"

    id = Column(Integer, primary_key=True, autoincrement=True)
    category_code = Column(String(50), nullable=False)
    category_name = Column(String(200), nullable=False)
    subcategory_code = Column(String(50), nullable=True)
    subcategory_name = Column(String(200), nullable=True)
    item_type_code = Column(String(50), nullable=True)
    item_type_name = Column(String(200), nullable=True)
    required_fields = Column(Text, nullable=False, default="[]")
    priority = Column(Integer, nullable=False, default=0)
    is_active = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime, nullable=False, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime, nullable=False, default=lambda: datetime.now(timezone.utc),
                        onupdate=lambda: datetime.now(timezone.utc))

    @property
    def required_fields_list(self) -> list[str]:
        if not self.required_fields:
            return []
        val = self.required_fields
        if isinstance(val, list):
            return val
        return json.loads(val)

    @required_fields_list.setter
    def required_fields_list(self, value: list[str]):
        self.required_fields = json.dumps(value, ensure_ascii=False)

    @property
    def display_name(self) -> str:
        parts = [self.category_name]
        if self.subcategory_name:
            parts.append(self.subcategory_name)
        if self.item_type_name:
            parts.append(self.item_type_name)
        return " / ".join(parts)


class ValidationRuleException(Base):
    """Exception override for a BaseValidationRule — matches by type name or standard."""

    __tablename__ = "validation_rule_exception"

    id = Column(Integer, primary_key=True, autoincrement=True)
    base_rule_id = Column(Integer, ForeignKey("base_validation_rule.id"), nullable=False)
    match_type_name = Column(String(200), nullable=True)
    match_standard = Column(String(100), nullable=True)
    override_required_fields = Column(Text, nullable=False, default="[]")
    note = Column(String(500), nullable=True)
    priority = Column(Integer, nullable=False, default=0)
    is_active = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime, nullable=False, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime, nullable=False, default=lambda: datetime.now(timezone.utc),
                        onupdate=lambda: datetime.now(timezone.utc))

    @property
    def override_required_fields_list(self) -> list[str]:
        if not self.override_required_fields:
            return []
        val = self.override_required_fields
        if isinstance(val, list):
            return val
        return json.loads(val)

    @override_required_fields_list.setter
    def override_required_fields_list(self, value: list[str]):
        self.override_required_fields = json.dumps(value, ensure_ascii=False)


# ── Coating rule engine ───────────────────────────────────────────────────────

class CoatingRule(Base):
    """DB-backed rule for coating detection from product name text."""

    __tablename__ = "coating_rule"

    id = Column(Integer, primary_key=True, autoincrement=True)
    pattern_raw = Column(String(200), nullable=False)
    match_type = Column(String(20), nullable=False, default="contains")  # contains | exact | regex
    coating_code = Column(String(50), nullable=False)
    coating_name = Column(String(200), nullable=False)
    priority = Column(Integer, nullable=False, default=0)
    is_active = Column(Boolean, nullable=False, default=True)
    note = Column(Text, nullable=True)
    created_at = Column(DateTime, nullable=False, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime, nullable=False, default=lambda: datetime.now(timezone.utc),
                        onupdate=lambda: datetime.now(timezone.utc))


# ── Strength rule engine ─────────────────────────────────────────────────────

class StrengthRule(Base):
    """DB-backed rule for strength class detection from product name text."""

    __tablename__ = "strength_rule"

    id = Column(Integer, primary_key=True, autoincrement=True)
    pattern_raw = Column(String(200), nullable=False)
    match_type = Column(String(20), nullable=False, default="contains")  # contains | exact | regex
    strength_code = Column(String(50), nullable=False)
    strength_name = Column(String(200), nullable=False)
    strength_family = Column(String(50), nullable=False, default="metric")  # metric | stainless
    priority = Column(Integer, nullable=False, default=0)
    is_active = Column(Boolean, nullable=False, default=True)
    note = Column(Text, nullable=True)
    created_at = Column(DateTime, nullable=False, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime, nullable=False, default=lambda: datetime.now(timezone.utc),
                        onupdate=lambda: datetime.now(timezone.utc))


# ── Size rule engine ─────────────────────────────────────────────────────────

class SizeRule(Base):
    """DB-backed rule for size/diameter/length detection from product name text.

    Regex patterns use named groups: (?P<d>...) for diameter, (?P<l>...) for length,
    (?P<w>...) for width, (?P<t>...) for thickness, (?P<pitch>...) for pitch,
    (?P<tol>...) for tolerance.

    normalize_template is a Python format string using group names, e.g. "M{d}x{l}".
    """

    __tablename__ = "size_rule"

    id = Column(Integer, primary_key=True, autoincrement=True)
    pattern_raw = Column(String(500), nullable=False)
    match_type = Column(String(20), nullable=False, default="regex")  # regex | contains | exact
    size_kind = Column(String(50), nullable=False)
    # diameter | diameter_length | triple_size | profile_size | thread | custom
    normalize_template = Column(String(200), nullable=True)  # e.g. "M{d}x{l}"
    priority = Column(Integer, nullable=False, default=0)
    is_active = Column(Boolean, nullable=False, default=True)
    note = Column(Text, nullable=True)
    created_at = Column(DateTime, nullable=False, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime, nullable=False, default=lambda: datetime.now(timezone.utc),
                        onupdate=lambda: datetime.now(timezone.utc))


# ── Unified normalization rules ──────────────────────────────────────────────

class NormalizationRule(Base):
    """Unified DB-backed rule for detecting and normalizing product attributes.

    rule_type: "coating" | "strength" | "size"
    match_type: "contains" | "exact" | "regex"
    extra_json: type-specific data as JSON (e.g. family, size_kind, normalize_template)

    For size rules, regex patterns use named groups: (?P<d>...) for diameter,
    (?P<l>...) for length, (?P<w>...) for width, (?P<t>...) for thickness,
    (?P<tol>...) for tolerance, (?P<pitch>...) for pitch.
    """

    __tablename__ = "normalization_rules"

    id = Column(Integer, primary_key=True, autoincrement=True)
    rule_type = Column(String(30), nullable=False, index=True)
    pattern_raw = Column(String(500), nullable=False)
    match_type = Column(String(20), nullable=False, default="contains")
    normalized_code = Column(String(200), nullable=False)
    normalized_name = Column(String(200), nullable=False)
    extra_json = Column(Text, nullable=True)
    priority = Column(Integer, nullable=False, default=0)
    is_active = Column(Boolean, nullable=False, default=True)
    note = Column(Text, nullable=True)
    created_at = Column(DateTime, nullable=False, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime, nullable=False, default=lambda: datetime.now(timezone.utc),
                        onupdate=lambda: datetime.now(timezone.utc))

    @property
    def extra(self) -> dict:
        try:
            return json.loads(self.extra_json or "{}")
        except (ValueError, TypeError):
            return {}

    @extra.setter
    def extra(self, value: dict) -> None:
        self.extra_json = json.dumps(value, ensure_ascii=False)
