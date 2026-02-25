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
