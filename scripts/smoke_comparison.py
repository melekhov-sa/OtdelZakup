"""Smoke-test the quote comparison API against a running service.

Exercises the three endpoints over real HTTP — something the unit tests, which
go through Starlette's TestClient, never touch: JSON encoding of Cyrillic,
Base64 payload size, actual status codes.

    python scripts/smoke_comparison.py
    python scripts/smoke_comparison.py http://Srv-sq-b1:8000

Creates one throwaway comparison named SMOKE-<pid> and leaves it in place;
it holds no real data and can be ignored or deleted later.
"""

import base64
import io
import os
import sys
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Catalog names are Russian; a cp1251/cp1252 console would abort on printing them
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")


def pick_catalog_uid() -> tuple[str, str]:
    """Return (uid_1c, name) of any active catalog item that has one."""
    import sqlite3

    db_path = os.environ.get("OTDELZAKUP_DB_PATH", "./data/readiness.db")
    conn = sqlite3.connect(db_path)
    row = conn.execute(
        "select uid_1c, name from internal_item "
        "where uid_1c is not null and uid_1c <> '' limit 1"
    ).fetchone()
    conn.close()
    if row is None:
        raise SystemExit("no catalog item with uid_1c — cannot smoke-test")
    return row[0], row[1]


def make_xlsx(name: str, price: float, unit: str) -> str:
    import openpyxl

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append(["Наименование", "Цена", "Ед"])
    ws.append([name, price, unit])
    buf = io.BytesIO()
    wb.save(buf)
    return base64.b64encode(buf.getvalue()).decode()


def main() -> int:
    base = sys.argv[1].rstrip("/") if len(sys.argv) > 1 else "http://127.0.0.1:8000"
    uid, name = pick_catalog_uid()
    ref = f"SMOKE-{os.getpid()}"
    print(f"service : {base}")
    print(f"position: {name[:60]}  uid={uid[:8]}...")

    r = requests.post(f"{base}/api/v1/comparison", json={
        "external_ref": ref,
        "title": "Смоук-тест сравнения",
        "positions": [{"uid_1c": uid, "qty": 100, "unit": "шт", "weight_kg": 0.085}],
    }, timeout=60)
    print(f"\n1. POST /comparison            -> {r.status_code} {r.text[:200]}")
    if r.status_code != 200:
        return 1
    cid = r.json()["comparison_id"]

    r = requests.post(f"{base}/api/v1/comparison/{cid}/quotes", json={
        "supplier": "ООО Смоук",
        "filename": "smoke.xlsx",
        "file_base64": make_xlsx(name, 167.0, "кг"),
    }, timeout=300)
    print(f"2. POST /comparison/{cid}/quotes -> {r.status_code} {r.text[:200]}")
    if r.status_code != 200:
        return 1

    r = requests.get(f"{base}/api/v1/comparison/{cid}", timeout=60)
    print(f"3. GET  /comparison/{cid}        -> {r.status_code}")
    if r.status_code != 200:
        print(r.text[:300])
        return 1

    body = r.json()
    print(f"\n   suppliers: {body['suppliers']}")
    for row in body["rows"]:
        print(f"   position : {row['name'][:50]}  {row['qty']} {row['unit']}")
        for supplier, cell in row["cells"].items():
            print(f"     {supplier}: {cell['price']} за {cell['unit']}"
                  f"  ->  {cell['price_normalized']} ({cell['basis']})"
                  f"{'  ПОДОЗРИТЕЛЬНО' if cell['suspicious'] else ''}")
        if not row["cells"]:
            print("     (ни один поставщик не предложил)")

    print("\nall three endpoints answered over real HTTP")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
