"""Save a settings snapshot to a file.

Usage:
    python scripts/export_settings.py D:\\OtdelZakup\\backups\\settings.json

Meant for the Windows task scheduler: the file it writes is the same one the
"Выгрузить настройки" button produces.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.settings_backup import build_snapshot, dump_snapshot  # noqa: E402


def main() -> int:
    if len(sys.argv) != 2:
        print("Usage: python scripts/export_settings.py <path-to-file.json>")
        return 2

    path = Path(sys.argv[1])
    path.parent.mkdir(parents=True, exist_ok=True)
    snapshot = build_snapshot()
    path.write_text(dump_snapshot(snapshot), encoding="utf-8")

    rows = sum(len(v) for v in snapshot["tables"].values())
    print(f"Снимок сохранён: {path} ({rows} записей)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
