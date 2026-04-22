"""Add category-based validation rules and exceptions.

Revision ID: 026
Revises: 025
Create Date: 2026-03-06
"""
from typing import Sequence, Union
import json

from alembic import op
import sqlalchemy as sa

revision: str = "026"
down_revision: Union[str, None] = "025"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "base_validation_rule",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("category_code", sa.String(50), nullable=False),
        sa.Column("category_name", sa.String(200), nullable=False),
        sa.Column("subcategory_code", sa.String(50), nullable=True),
        sa.Column("subcategory_name", sa.String(200), nullable=True),
        sa.Column("item_type_code", sa.String(50), nullable=True),
        sa.Column("item_type_name", sa.String(200), nullable=True),
        sa.Column("required_fields", sa.Text(), nullable=False, server_default="[]"),
        sa.Column("priority", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default="1"),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
    )

    op.create_table(
        "validation_rule_exception",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("base_rule_id", sa.Integer(), sa.ForeignKey("base_validation_rule.id"), nullable=False),
        sa.Column("match_type_name", sa.String(200), nullable=True),
        sa.Column("match_standard", sa.String(100), nullable=True),
        sa.Column("override_required_fields", sa.Text(), nullable=False, server_default="[]"),
        sa.Column("note", sa.String(500), nullable=True),
        sa.Column("priority", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default="1"),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
    )

    # ── Seed base rules ───────────────────────────────────────────────────────
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)

    tbl = sa.table(
        "base_validation_rule",
        sa.column("id", sa.Integer),
        sa.column("category_code", sa.String),
        sa.column("category_name", sa.String),
        sa.column("subcategory_code", sa.String),
        sa.column("subcategory_name", sa.String),
        sa.column("item_type_code", sa.String),
        sa.column("item_type_name", sa.String),
        sa.column("required_fields", sa.Text),
        sa.column("priority", sa.Integer),
        sa.column("is_active", sa.Boolean),
        sa.column("created_at", sa.DateTime),
        sa.column("updated_at", sa.DateTime),
    )

    exc = sa.table(
        "validation_rule_exception",
        sa.column("id", sa.Integer),
        sa.column("base_rule_id", sa.Integer),
        sa.column("match_type_name", sa.String),
        sa.column("match_standard", sa.String),
        sa.column("override_required_fields", sa.Text),
        sa.column("note", sa.String),
        sa.column("priority", sa.Integer),
        sa.column("is_active", sa.Boolean),
        sa.column("created_at", sa.DateTime),
        sa.column("updated_at", sa.DateTime),
    )

    def _jf(fields):
        return json.dumps(fields, ensure_ascii=False)

    rules = [
        # 1: Анкеры
        dict(id=1, category_code="anchors", category_name="Анкеры",
             required_fields=_jf(["type", "diameter", "length"]), priority=10),
        # 2: Болты фундаментные
        dict(id=2, category_code="foundation_bolts", category_name="Болты фундаментные",
             required_fields=_jf(["execution_type", "standard", "diameter", "length"]), priority=10),
        # 3: Дюбели
        dict(id=3, category_code="dowels", category_name="Дюбели",
             required_fields=_jf(["type", "diameter", "length"]), priority=10),
        # 4: Гвозди
        dict(id=4, category_code="nails_rivets", category_name="Заклепки и гвозди",
             subcategory_code="nails", subcategory_name="Гвозди",
             required_fields=_jf(["type", "diameter", "length"]), priority=10),
        # 5: Заклепки вытяжные
        dict(id=5, category_code="nails_rivets", category_name="Заклепки и гвозди",
             subcategory_code="blind_rivets", subcategory_name="Заклепки вытяжные",
             required_fields=_jf(["material", "diameter", "length"]), priority=10),
        # 6: Заклепки резьбовые
        dict(id=6, category_code="nails_rivets", category_name="Заклепки и гвозди",
             subcategory_code="threaded_rivets", subcategory_name="Заклепки резьбовые",
             required_fields=_jf(["shape", "flange_type", "diameter"]), priority=10),
        # 7: Мебельный крепеж — болты/винты/шурупы
        dict(id=7, category_code="furniture", category_name="Мебельный крепеж",
             item_type_code="bolt_screw", item_type_name="Болты, винты, шурупы",
             required_fields=_jf(["diameter", "length"]), priority=10),
        # 8: Мебельный крепеж — гайки
        dict(id=8, category_code="furniture", category_name="Мебельный крепеж",
             item_type_code="nut", item_type_name="Гайки",
             required_fields=_jf(["diameter"]), priority=10),
        # 9: Метрический крепеж — болты/винты/шпильки
        dict(id=9, category_code="metric", category_name="Метрический крепеж",
             item_type_code="bolt_screw_stud", item_type_name="Болты, винты, шпильки",
             required_fields=_jf(["standard", "strength_class", "coating", "diameter", "length"]), priority=10),
        # 10: Метрический крепеж — гайки
        dict(id=10, category_code="metric", category_name="Метрический крепеж",
             item_type_code="nut", item_type_name="Гайки",
             required_fields=_jf(["standard", "strength_class", "coating", "diameter"]), priority=10),
        # 11: Метрический крепеж — шайбы
        dict(id=11, category_code="metric", category_name="Метрический крепеж",
             item_type_code="washer", item_type_name="Шайбы",
             required_fields=_jf(["standard", "coating", "diameter"]), priority=10),
        # 12: Нержавеющая сталь — заклепки
        dict(id=12, category_code="stainless", category_name="Нержавеющая сталь",
             subcategory_code="rivets", subcategory_name="Заклепки нержавеющая сталь",
             required_fields=_jf(["steel_grade", "diameter", "length"]), priority=10),
        # 13: Нержавеющая сталь — болты/винты/шпильки
        dict(id=13, category_code="stainless", category_name="Нержавеющая сталь",
             subcategory_code="metric", subcategory_name="Метрика нержавеющая сталь",
             item_type_code="bolt_screw_stud", item_type_name="Болты, винты, шпильки",
             required_fields=_jf(["standard", "steel_grade", "diameter", "length"]), priority=10),
        # 14: Нержавеющая сталь — гайки
        dict(id=14, category_code="stainless", category_name="Нержавеющая сталь",
             subcategory_code="metric", subcategory_name="Метрика нержавеющая сталь",
             item_type_code="nut", item_type_name="Гайки",
             required_fields=_jf(["standard", "steel_grade", "diameter"]), priority=10),
        # 15: Нержавеющая сталь — шайбы
        dict(id=15, category_code="stainless", category_name="Нержавеющая сталь",
             subcategory_code="metric", subcategory_name="Метрика нержавеющая сталь",
             item_type_code="washer", item_type_name="Шайбы",
             required_fields=_jf(["standard", "steel_grade", "diameter"]), priority=10),
        # 16: Штифты и шплинты нерж.
        dict(id=16, category_code="stainless", category_name="Нержавеющая сталь",
             subcategory_code="pins_cotters", subcategory_name="Штифты и шплинты",
             required_fields=_jf(["standard", "steel_grade", "diameter", "length"]), priority=10),
        # 17: Шурупы и саморезы нерж.
        dict(id=17, category_code="stainless", category_name="Нержавеющая сталь",
             subcategory_code="screws_stainless", subcategory_name="Шурупы и саморезы нержавеющая сталь",
             required_fields=_jf(["standard", "steel_grade", "diameter", "length"]), priority=10),
        # 18: Нержавеющий такелаж
        dict(id=18, category_code="stainless_rigging", category_name="Нержавеющий такелаж",
             required_fields=_jf(["standard", "steel_grade", "diameter"]), priority=10),
        # 19: Перфорированный крепеж — ленты
        dict(id=19, category_code="perforated", category_name="Перфорированный крепеж",
             subcategory_code="tapes", subcategory_name="Ленты",
             required_fields=_jf(["type", "thickness", "length", "width"]), priority=10),
        # 20: Перфорированный крепеж — опоры и держатели
        dict(id=20, category_code="perforated", category_name="Перфорированный крепеж",
             subcategory_code="supports", subcategory_name="Опоры и держатели",
             required_fields=_jf(["type", "size"]), priority=10),
        # 21: Перфорированный крепеж — пластины
        dict(id=21, category_code="perforated", category_name="Перфорированный крепеж",
             subcategory_code="plates", subcategory_name="Пластины",
             required_fields=_jf(["type", "size"]), priority=10),
        # 22: Перфорированный крепеж — профиль монтажный
        dict(id=22, category_code="perforated", category_name="Перфорированный крепеж",
             subcategory_code="profile", subcategory_name="Профиль монтажный",
             required_fields=_jf(["type", "diameter", "length"]), priority=10),
        # 23: Перфорированный крепеж — уголки
        dict(id=23, category_code="perforated", category_name="Перфорированный крепеж",
             subcategory_code="angles", subcategory_name="Уголки",
             required_fields=_jf(["type", "width", "length", "thickness"]), priority=10),
        # 24: Саморезы DIN
        dict(id=24, category_code="screws_din", category_name="Саморезы DIN",
             required_fields=_jf(["standard", "diameter", "length"]), priority=10),
        # 25: Саморезы и шурупы
        dict(id=25, category_code="screws", category_name="Саморезы и шурупы",
             required_fields=_jf(["type", "diameter", "length"]), priority=10),
        # 26: Стяжки
        dict(id=26, category_code="ties_clamps", category_name="Стяжки скобы хомуты",
             subcategory_code="ties", subcategory_name="Стяжки",
             required_fields=_jf(["material", "diameter", "length"]), priority=10),
        # 27: Хомуты
        dict(id=27, category_code="ties_clamps", category_name="Стяжки скобы хомуты",
             subcategory_code="clamps", subcategory_name="Хомуты",
             required_fields=_jf(["type", "diameter"]), priority=10),
        # 28: Грузоподъемные приспособления
        dict(id=28, category_code="rigging", category_name="Такелаж и грузоподъемные приспособления",
             subcategory_code="lifting", subcategory_name="Грузоподъемные приспособления",
             required_fields=_jf(["type", "diameter", "load_capacity"]), priority=10),
        # 29: Такелаж
        dict(id=29, category_code="rigging", category_name="Такелаж и грузоподъемные приспособления",
             subcategory_code="rigging", subcategory_name="Такелаж",
             required_fields=_jf(["type", "size"]), priority=10),
        # 30: Цепи Тросы Шнуры
        dict(id=30, category_code="rigging", category_name="Такелаж и грузоподъемные приспособления",
             subcategory_code="chains_ropes", subcategory_name="Цепи Тросы Шнуры",
             required_fields=_jf(["standard", "diameter"]), priority=10),
        # 31: Фиксаторы арматуры
        dict(id=31, category_code="rebar_spacers", category_name="Фиксаторы арматуры",
             required_fields=_jf(["type", "size"]), priority=10),
        # 32: Штифты шплинты
        dict(id=32, category_code="pins_cotters", category_name="Штифты шплинты",
             required_fields=_jf(["standard", "diameter", "length"]), priority=10),
    ]

    for r in rules:
        r.setdefault("subcategory_code", None)
        r.setdefault("subcategory_name", None)
        r.setdefault("item_type_code", None)
        r.setdefault("item_type_name", None)
        r["is_active"] = True
        r["created_at"] = now
        r["updated_at"] = now

    op.bulk_insert(tbl, rules)

    # ── Seed exceptions ───────────────────────────────────────────────────────
    exceptions = [
        # Анкер забиваемый стальной → только diameter
        dict(base_rule_id=1, match_type_name="анкер забиваемый стальной",
             override_required_fields=_jf(["diameter"]),
             note="Анкер забиваемый стальной — только диаметр", priority=10),
        # Забивной анкер латунный → только diameter
        dict(base_rule_id=1, match_type_name="забивной анкер латунный",
             override_required_fields=_jf(["diameter"]),
             note="Забивной анкер латунный — только диаметр", priority=10),
        # Дюбели с полукольцом KRHS → только diameter
        dict(base_rule_id=3, match_type_name="дюбель с полукольцом",
             override_required_fields=_jf(["diameter"]),
             note="Дюбели с полукольцом KRHS — только диаметр", priority=10),
        # Дюбели с прямым крюком KRHP → только diameter
        dict(base_rule_id=3, match_type_name="дюбель с прямым крюком",
             override_required_fields=_jf(["diameter"]),
             note="Дюбели с прямым крюком KRHP — только диаметр", priority=10),
        # Штифты DIN 11024 → только diameter
        dict(base_rule_id=32, match_standard="DIN 11024",
             override_required_fields=_jf(["diameter"]),
             note="DIN 11024 — длина не требуется", priority=10),
        # Штифты DIN 94 → дополнительно coating
        dict(base_rule_id=32, match_standard="DIN 94",
             override_required_fields=_jf(["standard", "diameter", "length", "coating"]),
             note="DIN 94 — дополнительно обязательно покрытие", priority=10),
    ]

    for e in exceptions:
        e.setdefault("match_type_name", None)
        e.setdefault("match_standard", None)
        e["is_active"] = True
        e["created_at"] = now
        e["updated_at"] = now

    op.bulk_insert(exc, exceptions)


def downgrade() -> None:
    op.drop_table("validation_rule_exception")
    op.drop_table("base_validation_rule")
