"""ORM models for the application."""

import json
from datetime import datetime, timezone

from sqlalchemy import Boolean, Column, DateTime, Integer, String, Text, UniqueConstraint

from app.database import Base


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
