"""Compare the live database schema against the current ORM models.

The service historically created its tables with ``Base.metadata.create_all()``
instead of running alembic.  ``create_all`` adds missing *tables* but never adds
*columns* to tables that already exist, so a long-lived database drifts away
from the models one migration at a time — silently, until a query asks for a
column that was never created.

    python scripts/check_schema.py                  # report only, changes nothing
    python scripts/check_schema.py --apply          # add the missing columns
    python scripts/check_schema.py path\to.db --apply

``--apply`` refuses to run when a column cannot be appended safely, and it also
creates the indexes declared on the columns it adds.  Take a copy of the
database first — SQLite gives no transaction to roll a bad DDL run back into.
"""

import sys
from pathlib import Path

from sqlalchemy import create_engine, inspect

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.database import Base  # noqa: E402
import app.models  # noqa: E402,F401  — registers the main tables
import app.order_models  # noqa: E402,F401  — registers orders/quotes


def main() -> int:
    apply_changes = "--apply" in sys.argv
    args = [a for a in sys.argv[1:] if a != "--apply"]
    db_path = Path(args[0] if args else "./data/readiness.db")

    if not db_path.exists():
        print(f"database not found: {db_path}")
        return 2

    engine = create_engine(f"sqlite:///{db_path}")
    insp = inspect(engine)
    present = set(insp.get_table_names())

    missing_tables: list[str] = []
    missing_columns: list[tuple] = []
    missing_indexes: list[tuple] = []
    unaddable: list[str] = []

    for table in Base.metadata.sorted_tables:
        if table.name not in present:
            missing_tables.append(table.name)
            continue
        have = {c["name"] for c in insp.get_columns(table.name)}
        known_indexes = {i["name"] for i in insp.get_indexes(table.name)}
        for col in table.columns:
            if col.name in have:
                continue
            # SQLite can only append a column that tolerates existing rows
            if not col.nullable and col.server_default is None and col.default is None:
                unaddable.append(f"{table.name}.{col.name}")
                continue
            missing_columns.append((table.name, col.name, col.type, col.nullable))
            index_name = f"ix_{table.name}_{col.name}"
            if col.index and index_name not in known_indexes:
                missing_indexes.append((table.name, col.name, index_name))

    print(f"database: {db_path}  ({len(present)} tables)")
    print(f"missing tables : {len(missing_tables)}")
    print(f"missing columns: {len(missing_columns)}")
    print(f"missing indexes: {len(missing_indexes)}")
    print(f"needs rebuild  : {len(unaddable)}")

    if missing_tables:
        print("\n-- tables absent (create_all makes these on next start) --")
        for name in missing_tables:
            print(f"   {name}")

    if missing_columns:
        print("\n-- columns absent --")
        for name, col, coltype, nullable in missing_columns:
            null_sql = "" if nullable else " NOT NULL"
            print(f"ALTER TABLE {name} ADD COLUMN {col} {coltype}{null_sql};")

    if missing_indexes:
        print("\n-- indexes absent --")
        for name, col, index_name in missing_indexes:
            print(f"CREATE INDEX {index_name} ON {name} ({col});")

    if unaddable:
        print("\n-- NOT NULL without default: cannot be appended, needs a rebuild --")
        for item in unaddable:
            print(f"   {item}")

    if not (missing_tables or missing_columns or missing_indexes or unaddable):
        print("\nschema matches the models")
        return 0

    if not apply_changes:
        print("\nnothing changed. re-run with --apply to add them")
        return 0

    if unaddable:
        print("\nrefusing to apply: some columns need a table rebuild")
        return 1

    print("\n-- applying --")
    with engine.begin() as conn:
        for name, col, coltype, nullable in missing_columns:
            null_sql = "" if nullable else " NOT NULL"
            conn.exec_driver_sql(f"ALTER TABLE {name} ADD COLUMN {col} {coltype}{null_sql}")
            print(f"   + column {name}.{col}")
        for name, col, index_name in missing_indexes:
            conn.exec_driver_sql(f"CREATE INDEX IF NOT EXISTS {index_name} ON {name} ({col})")
            print(f"   + index  {index_name}")

    print("\ndone. now run:  python -m alembic stamp head")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
