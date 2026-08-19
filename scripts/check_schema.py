"""Compare the live database schema against the current ORM models.

The service historically created its tables with ``Base.metadata.create_all()``
instead of running alembic.  ``create_all`` adds missing *tables* but never adds
*columns* to tables that already exist, so a long-lived database drifts away
from the models one migration at a time — silently, until a query asks for a
column that was never created.

This script reports the drift and prints the ALTER statements that would fix it.
It changes nothing: run it, read it, decide.

    python scripts/check_schema.py            # default ./data/readiness.db
    python scripts/check_schema.py path\to.db
"""

import sys
from pathlib import Path

from sqlalchemy import create_engine, inspect

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.database import Base  # noqa: E402
import app.models  # noqa: E402,F401  — registers the main tables
import app.order_models  # noqa: E402,F401  — registers orders/quotes


def main() -> int:
    db_path = Path(sys.argv[1] if len(sys.argv) > 1 else "./data/readiness.db")
    if not db_path.exists():
        print(f"database not found: {db_path}")
        return 2

    engine = create_engine(f"sqlite:///{db_path}")
    insp = inspect(engine)
    present = set(insp.get_table_names())

    missing_tables: list[str] = []
    missing_columns: list[tuple] = []
    unaddable: list[str] = []

    for table in Base.metadata.sorted_tables:
        if table.name not in present:
            missing_tables.append(table.name)
            continue
        have = {c["name"] for c in insp.get_columns(table.name)}
        for col in table.columns:
            if col.name in have:
                continue
            # SQLite can only append a column that tolerates existing rows
            if not col.nullable and col.server_default is None and col.default is None:
                unaddable.append(f"{table.name}.{col.name}")
            else:
                missing_columns.append((table.name, col.name, col.type, col.nullable))

    print(f"database: {db_path}  ({len(present)} tables)")
    print(f"missing tables : {len(missing_tables)}")
    print(f"missing columns: {len(missing_columns)}")
    print(f"needs rebuild  : {len(unaddable)}")

    if missing_tables:
        print("\n-- tables absent (create_all makes these on next start) --")
        for name in missing_tables:
            print(f"   {name}")

    if missing_columns:
        print("\n-- columns absent; SQL to add them --")
        for name, col, coltype, nullable in missing_columns:
            null_sql = "" if nullable else " NOT NULL"
            print(f"ALTER TABLE {name} ADD COLUMN {col} {coltype}{null_sql};")

    if unaddable:
        print("\n-- NOT NULL without default: cannot be appended, needs a rebuild --")
        for item in unaddable:
            print(f"   {item}")

    if not (missing_tables or missing_columns or unaddable):
        print("\nschema matches the models")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
