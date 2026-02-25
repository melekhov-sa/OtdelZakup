"""ORM models for the application."""

import json
from datetime import datetime, timezone

from sqlalchemy import Boolean, Column, DateTime, Integer, String, Text

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
