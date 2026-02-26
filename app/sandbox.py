"""Sandbox Mode — snapshot-based rule isolation for safe experimentation.

A SandboxSession captures a point-in-time JSON snapshot of all rules.
Processing in sandbox mode uses snapshot rules instead of the live DB,
so rule changes inside a sandbox never affect the production configuration.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional


# ── Snapshot serialization (DB → JSON) ───────────────────────────────────────

def _rule_to_dict(r, fields: list[str]) -> dict:
    return {f: getattr(r, f, None) for f in fields}


_READINESS_FIELDS = ["id", "name", "description", "item_type", "require_fields", "priority", "is_active"]
_VALIDATION_FIELDS = [
    "id", "name", "description", "item_type",
    "require_fields", "forbid_fields", "force_status",
    "condition_type", "standard_source", "expected_item_type_mode", "expected_item_type",
    "priority", "is_active",
]
_INFERENCE_FIELDS = ["id", "name", "is_active", "target_field", "item_types", "mode", "conditions_json", "priority"]
_STANDARD_FIELDS = ["id", "standard_kind", "standard_code", "title", "item_type", "notes", "is_active"]


def take_snapshot() -> str:
    """Read all rules from DB (active + inactive) and return a JSON snapshot string."""
    from app.database import get_db_session
    from app.models import InferenceRule, ReadinessRule, StandardRef, ValidationRule

    session = get_db_session()
    try:
        snapshot = {
            "readiness_rules": [
                _rule_to_dict(r, _READINESS_FIELDS)
                for r in session.query(ReadinessRule).order_by(ReadinessRule.priority.asc(), ReadinessRule.id).all()
            ],
            "validation_rules": [
                _rule_to_dict(r, _VALIDATION_FIELDS)
                for r in session.query(ValidationRule).order_by(ValidationRule.priority.asc(), ValidationRule.id).all()
            ],
            "inference_rules": [
                _rule_to_dict(r, _INFERENCE_FIELDS)
                for r in session.query(InferenceRule).order_by(InferenceRule.priority.asc(), InferenceRule.id).all()
            ],
            "standard_refs": [
                _rule_to_dict(r, _STANDARD_FIELDS)
                for r in session.query(StandardRef).order_by(StandardRef.standard_kind, StandardRef.standard_code).all()
            ],
        }
        return json.dumps(snapshot, ensure_ascii=False, default=str)
    finally:
        session.close()


# ── Sandbox rule wrapper objects (compatible with rule engines) ────────────────

@dataclass
class _SandboxReadinessRule:
    id: int = 0
    name: str = ""
    description: str = ""
    item_type: Optional[str] = None
    require_fields: str = "[]"
    priority: int = 0
    is_active: bool = True

    @property
    def require_fields_list(self) -> list:
        if isinstance(self.require_fields, list):
            return self.require_fields
        try:
            return json.loads(self.require_fields) if self.require_fields else []
        except (json.JSONDecodeError, TypeError):
            return []

    @require_fields_list.setter
    def require_fields_list(self, value: list):
        self.require_fields = json.dumps(value, ensure_ascii=False)


@dataclass
class _SandboxValidationRule:
    id: int = 0
    name: str = ""
    description: str = ""
    item_type: Optional[str] = None
    require_fields: str = "[]"
    forbid_fields: str = "[]"
    force_status: Optional[str] = None
    condition_type: str = "FIELDS_REQUIRED"
    standard_source: str = "ANY"
    expected_item_type_mode: str = "FROM_DIRECTORY"
    expected_item_type: Optional[str] = None
    priority: int = 0
    is_active: bool = True

    @property
    def require_fields_list(self) -> list:
        if isinstance(self.require_fields, list):
            return self.require_fields
        try:
            return json.loads(self.require_fields) if self.require_fields else []
        except (json.JSONDecodeError, TypeError):
            return []

    @require_fields_list.setter
    def require_fields_list(self, value: list):
        self.require_fields = json.dumps(value, ensure_ascii=False)

    @property
    def forbid_fields_list(self) -> list:
        if isinstance(self.forbid_fields, list):
            return self.forbid_fields
        try:
            return json.loads(self.forbid_fields) if self.forbid_fields else []
        except (json.JSONDecodeError, TypeError):
            return []

    @forbid_fields_list.setter
    def forbid_fields_list(self, value: list):
        self.forbid_fields = json.dumps(value, ensure_ascii=False)

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


@dataclass
class _SandboxInferenceRule:
    id: int = 0
    name: str = ""
    is_active: bool = True
    target_field: str = "size"
    item_types: Optional[str] = None
    mode: str = "DIAMETER_AS_SIZE"
    conditions_json: Optional[str] = None
    priority: int = 0

    @property
    def item_types_list(self) -> list:
        if not self.item_types:
            return []
        if isinstance(self.item_types, list):
            return self.item_types
        try:
            return json.loads(self.item_types)
        except (json.JSONDecodeError, TypeError):
            return []

    @property
    def mode_label(self) -> str:
        labels = {
            "DIAMETER_AS_SIZE": "Размер = Диаметр",
            "DIAMETER_X_LENGTH_AS_SIZE": "Размер = Диаметр × Длина",
        }
        return labels.get(self.mode, self.mode)


@dataclass
class _SandboxStandardRef:
    id: int = 0
    standard_kind: str = ""
    standard_code: str = ""
    title: Optional[str] = None
    item_type: Optional[str] = None
    notes: Optional[str] = None
    is_active: bool = True


def _from_dict(cls, d: dict):
    """Instantiate a dataclass from a dict, ignoring unknown keys."""
    known = cls.__dataclass_fields__.keys()
    return cls(**{k: v for k, v in d.items() if k in known})


# ── Load snapshot → rule context ──────────────────────────────────────────────

def load_snapshot_rules(snapshot_json: str) -> dict:
    """Parse snapshot JSON into rule context dict.

    Returns:
        {
            "readiness_rules":  list of _SandboxReadinessRule  (active only),
            "validation_rules": list of _SandboxValidationRule (active only),
            "inference_rules":  list of _SandboxInferenceRule  (active only),
            "standards_cache":  {(kind, code): (item_type, title)},
        }
    """
    data = json.loads(snapshot_json)

    readiness_rules = [
        _from_dict(_SandboxReadinessRule, r)
        for r in data.get("readiness_rules", [])
        if r.get("is_active", True)
    ]
    validation_rules = [
        _from_dict(_SandboxValidationRule, r)
        for r in data.get("validation_rules", [])
        if r.get("is_active", True)
    ]
    inference_rules = [
        _from_dict(_SandboxInferenceRule, r)
        for r in data.get("inference_rules", [])
        if r.get("is_active", True)
    ]

    standards_cache: dict = {}
    for s in data.get("standard_refs", []):
        if s.get("is_active", True):
            key = (s.get("standard_kind", ""), s.get("standard_code", ""))
            standards_cache[key] = (s.get("item_type") or "", s.get("title") or "")

    return {
        "readiness_rules": readiness_rules,
        "validation_rules": validation_rules,
        "inference_rules": inference_rules,
        "standards_cache": standards_cache,
    }


# ── Session lifecycle ─────────────────────────────────────────────────────────

def create_sandbox_session(description: str = "") -> int:
    """Snapshot current rules and persist a new SandboxSession. Returns session id."""
    from app.database import get_db_session
    from app.models import SandboxSession

    snapshot_json = take_snapshot()
    label = description or f"Snapshot от {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')}"
    db = get_db_session()
    try:
        sb = SandboxSession(
            base_version=label,
            rule_snapshot_json=snapshot_json,
            is_active=True,
            is_applied=False,
        )
        db.add(sb)
        db.commit()
        db.refresh(sb)
        return sb.id
    finally:
        db.close()


def apply_snapshot_to_prod(snapshot_json: str, description: str = "") -> int:
    """Replace all prod rules with snapshot contents. Creates a RuleVersion record.

    Returns the new RuleVersion id.
    """
    from app.database import get_db_session
    from app.models import (
        InferenceRule, ReadinessRule, RuleVersion,
        StandardRef, ValidationRule,
    )

    data = json.loads(snapshot_json)
    label = description or f"Sandbox применён {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')}"

    db = get_db_session()
    try:
        # Save history record first
        rv = RuleVersion(description=label, snapshot_json=snapshot_json)
        db.add(rv)

        # Replace readiness rules
        db.query(ReadinessRule).delete()
        for r in data.get("readiness_rules", []):
            db.add(ReadinessRule(
                name=r["name"],
                description=r.get("description", ""),
                item_type=r.get("item_type"),
                require_fields=r.get("require_fields", "[]"),
                priority=r.get("priority", 0),
                is_active=r.get("is_active", True),
            ))

        # Replace validation rules
        db.query(ValidationRule).delete()
        for r in data.get("validation_rules", []):
            db.add(ValidationRule(
                name=r["name"],
                description=r.get("description", ""),
                item_type=r.get("item_type"),
                require_fields=r.get("require_fields", "[]"),
                forbid_fields=r.get("forbid_fields", "[]"),
                force_status=r.get("force_status"),
                condition_type=r.get("condition_type", "FIELDS_REQUIRED"),
                standard_source=r.get("standard_source", "ANY"),
                expected_item_type_mode=r.get("expected_item_type_mode", "FROM_DIRECTORY"),
                expected_item_type=r.get("expected_item_type"),
                priority=r.get("priority", 0),
                is_active=r.get("is_active", True),
            ))

        # Replace inference rules
        db.query(InferenceRule).delete()
        for r in data.get("inference_rules", []):
            db.add(InferenceRule(
                name=r["name"],
                is_active=r.get("is_active", True),
                target_field=r.get("target_field", "size"),
                item_types=r.get("item_types"),
                mode=r.get("mode", "DIAMETER_AS_SIZE"),
                conditions_json=r.get("conditions_json"),
                priority=r.get("priority", 0),
            ))

        # Replace standard refs
        db.query(StandardRef).delete()
        for r in data.get("standard_refs", []):
            db.add(StandardRef(
                standard_kind=r["standard_kind"],
                standard_code=r["standard_code"],
                title=r.get("title"),
                item_type=r.get("item_type"),
                notes=r.get("notes"),
                is_active=r.get("is_active", True),
            ))

        db.commit()
        db.refresh(rv)
        return rv.id
    finally:
        db.close()


# ── Snapshot mutation helpers ─────────────────────────────────────────────────

def _next_id(items: list[dict]) -> int:
    """Return max(id) + 1 for a list of rule dicts."""
    if not items:
        return 1
    return max((r.get("id") or 0) for r in items) + 1


def snapshot_add_rule(snapshot_json: str, category: str, rule_dict: dict) -> str:
    """Add a rule dict to the given category list in the snapshot. Returns updated JSON."""
    data = json.loads(snapshot_json)
    lst = data.setdefault(category, [])
    rule_dict["id"] = _next_id(lst)
    lst.append(rule_dict)
    return json.dumps(data, ensure_ascii=False, default=str)


def snapshot_update_rule(snapshot_json: str, category: str, rule_id: int, updates: dict) -> str:
    """Update a rule in the snapshot by id. Returns updated JSON."""
    data = json.loads(snapshot_json)
    for r in data.get(category, []):
        if r.get("id") == rule_id:
            r.update(updates)
            break
    return json.dumps(data, ensure_ascii=False, default=str)


def snapshot_delete_rule(snapshot_json: str, category: str, rule_id: int) -> str:
    """Remove a rule from the snapshot by id. Returns updated JSON."""
    data = json.loads(snapshot_json)
    data[category] = [r for r in data.get(category, []) if r.get("id") != rule_id]
    return json.dumps(data, ensure_ascii=False, default=str)


def snapshot_toggle_rule(snapshot_json: str, category: str, rule_id: int) -> str:
    """Toggle is_active for a rule in the snapshot. Returns updated JSON."""
    data = json.loads(snapshot_json)
    for r in data.get(category, []):
        if r.get("id") == rule_id:
            r["is_active"] = not r.get("is_active", True)
            break
    return json.dumps(data, ensure_ascii=False, default=str)


def get_snapshot_rule(snapshot_json: str, category: str, rule_id: int) -> Optional[dict]:
    """Return a single rule dict from snapshot by id, or None."""
    data = json.loads(snapshot_json)
    for r in data.get(category, []):
        if r.get("id") == rule_id:
            return dict(r)
    return None


def get_snapshot_list(snapshot_json: str, category: str) -> list[dict]:
    """Return a list of rule dicts for the given category."""
    return json.loads(snapshot_json).get(category, [])


# ── Sandbox DB helpers ────────────────────────────────────────────────────────

def get_sandbox(session_id: int):
    """Load SandboxSession from DB. Returns None if not found or not active."""
    from app.database import get_db_session
    from app.models import SandboxSession

    db = get_db_session()
    try:
        sb = db.get(SandboxSession, session_id)
        if sb is not None:
            db.expunge(sb)
        return sb
    finally:
        db.close()


def update_sandbox_snapshot(session_id: int, new_snapshot_json: str) -> bool:
    """Persist an updated snapshot JSON. Returns True on success."""
    from app.database import get_db_session
    from app.models import SandboxSession

    db = get_db_session()
    try:
        sb = db.get(SandboxSession, session_id)
        if sb is None:
            return False
        sb.rule_snapshot_json = new_snapshot_json
        db.commit()
        return True
    finally:
        db.close()


def update_sandbox_file_id(session_id: int, file_id: str) -> None:
    """Store the last-processed file_id in the sandbox session."""
    from app.database import get_db_session
    from app.models import SandboxSession

    db = get_db_session()
    try:
        sb = db.get(SandboxSession, session_id)
        if sb is not None:
            sb.last_file_id = file_id
            db.commit()
    finally:
        db.close()
