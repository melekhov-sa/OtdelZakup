"""Settings snapshot: collect, restore, invalidate caches, back up."""
import json

import pytest

from app.models import ReadinessRule, StandardEquivalent


@pytest.fixture(autouse=True)
def _set_dirs(tmp_path, monkeypatch):
    upload_dir = tmp_path / "uploads"
    cache_dir = tmp_path / "cache"
    monkeypatch.setenv("OTDELZAKUP_UPLOAD_DIR", str(upload_dir))
    monkeypatch.setenv("OTDELZAKUP_CACHE_DIR", str(cache_dir))
    import app.cache as cache_mod
    cache_mod.UPLOAD_DIR = upload_dir
    cache_mod.CACHE_DIR = cache_dir

    db_path = tmp_path / "test.db"
    monkeypatch.setenv("OTDELZAKUP_DB_PATH", str(db_path))
    import app.database as db_mod
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    db_mod.DB_PATH = db_path
    db_mod.engine = create_engine(f"sqlite:///{db_path}", connect_args={"check_same_thread": False})
    db_mod.SessionLocal = sessionmaker(bind=db_mod.engine, autoflush=False, expire_on_commit=False)
    db_mod.init_db()


def _add_equiv(src="GOST-7798-70", dst="DIN-933"):
    from app.database import get_db_session
    session = get_db_session()
    session.add(StandardEquivalent(src_canonical=src, dst_canonical=dst, is_active=True))
    session.commit()
    session.close()


def test_snapshot_has_header():
    from app.settings_backup import FORMAT_VERSION, build_snapshot
    snap = build_snapshot()
    assert snap["format_version"] == FORMAT_VERSION
    assert snap["exported_at"]
    assert "source_host" in snap
    assert isinstance(snap["tables"], dict)


def test_snapshot_covers_every_model():
    from app.settings_backup import SNAPSHOT_MODELS, build_snapshot
    snap = build_snapshot()
    assert len(SNAPSHOT_MODELS) == 17
    for model in SNAPSHOT_MODELS:
        assert model.__tablename__ in snap["tables"]


def test_snapshot_excludes_catalog_and_match_memory():
    from app.settings_backup import build_snapshot
    tables = build_snapshot()["tables"]
    for forbidden in ("internal_item", "nomenclature_folder", "supplier_internal_match",
                      "orders", "quotes"):
        assert forbidden not in tables


def test_snapshot_keeps_rows_with_their_ids():
    _add_equiv()
    from app.settings_backup import build_snapshot
    rows = build_snapshot()["tables"]["standard_equivalents"]
    assert len(rows) == 1
    assert rows[0]["id"] == 1
    assert rows[0]["src_canonical"] == "GOST-7798-70"
    assert rows[0]["dst_canonical"] == "DIN-933"


def test_dates_are_serialized_as_text():
    from app.database import get_db_session
    from app.settings_backup import build_snapshot, dump_snapshot
    session = get_db_session()
    session.add(ReadinessRule(name="Проверка", item_type="болт", priority=1, is_active=True))
    session.commit()
    session.close()

    snap = build_snapshot()
    # Must survive json.dumps — a raw datetime would raise here.
    text = dump_snapshot(snap)
    assert json.loads(text)["tables"]["readiness_rule"][0]["name"] == "Проверка"


def test_dump_is_readable_utf8():
    _add_equiv()
    from app.settings_backup import build_snapshot, dump_snapshot
    text = dump_snapshot(build_snapshot())
    assert "GOST-7798-70" in text
    assert "\\u0413" not in text  # Cyrillic must not be escaped


def test_restore_fills_empty_database():
    _add_equiv()
    from app.settings_backup import build_snapshot, restore_snapshot
    snap = build_snapshot()

    from app.database import get_db_session
    session = get_db_session()
    session.query(StandardEquivalent).delete()
    session.commit()
    assert session.query(StandardEquivalent).count() == 0
    session.close()

    report = restore_snapshot(snap)

    session = get_db_session()
    assert session.query(StandardEquivalent).count() == 1
    session.close()
    assert report["tables"]["standard_equivalents"]["after"] == 1


def test_restore_replaces_existing_rows():
    _add_equiv("GOST-5927-70", "DIN-934")
    from app.settings_backup import build_snapshot, restore_snapshot
    snap = build_snapshot()

    _add_equiv("GOST-7798-70", "DIN-933")  # local addition, must not survive

    restore_snapshot(snap)

    from app.database import get_db_session
    session = get_db_session()
    rows = session.query(StandardEquivalent).all()
    pairs = {(r.src_canonical, r.dst_canonical) for r in rows}
    session.close()
    assert pairs == {("GOST-5927-70", "DIN-934")}


def test_absent_table_is_left_alone():
    _add_equiv()
    from app.settings_backup import restore_snapshot
    payload = {"format_version": 1, "tables": {"product_type": []}}

    restore_snapshot(payload)

    from app.database import get_db_session
    session = get_db_session()
    assert session.query(StandardEquivalent).count() == 1
    session.close()


def test_empty_list_clears_the_table():
    _add_equiv()
    from app.settings_backup import restore_snapshot
    restore_snapshot({"format_version": 1, "tables": {"standard_equivalents": []}})

    from app.database import get_db_session
    session = get_db_session()
    assert session.query(StandardEquivalent).count() == 0
    session.close()


def test_unknown_format_version_is_refused():
    _add_equiv()
    from app.settings_backup import SnapshotError, restore_snapshot
    with pytest.raises(SnapshotError):
        restore_snapshot({"format_version": 999, "tables": {"standard_equivalents": []}})

    from app.database import get_db_session
    session = get_db_session()
    assert session.query(StandardEquivalent).count() == 1  # untouched
    session.close()


def test_payload_without_tables_is_refused():
    from app.settings_backup import SnapshotError, restore_snapshot
    with pytest.raises(SnapshotError):
        restore_snapshot({"format_version": 1})


def test_restore_reports_before_and_after():
    _add_equiv("GOST-5927-70", "DIN-934")
    from app.settings_backup import build_snapshot, restore_snapshot
    snap = build_snapshot()
    _add_equiv("GOST-7798-70", "DIN-933")

    report = restore_snapshot(snap)
    assert report["tables"]["standard_equivalents"] == {"before": 2, "after": 1}


def test_restore_leaves_the_catalog_alone():
    """The catalog comes from 1C — a settings snapshot must not touch it."""
    from datetime import datetime, timezone
    from app.database import get_db_session
    from app.models import InternalItem

    session = get_db_session()
    session.add(InternalItem(
        name="Болт М12x60 DIN 933", item_type="болт", size="M12x60",
        standard_text="DIN 933", standard_key="DIN-933", is_active=True,
        created_at=datetime.now(timezone.utc), updated_at=datetime.now(timezone.utc),
    ))
    session.commit()
    session.close()

    from app.settings_backup import restore_snapshot
    restore_snapshot({"format_version": 1, "tables": {"standard_equivalents": []}})

    session = get_db_session()
    assert session.query(InternalItem).count() == 1
    session.close()


def test_restore_drops_the_analog_cache():
    """A stale cache would keep serving the pairs that were just replaced."""
    from app.matching.standard_analogs import get_standard_analogs
    _add_equiv("GOST-7798-70", "DIN-933")
    from app.settings_backup import build_snapshot, restore_snapshot
    snap = build_snapshot()

    assert get_standard_analogs("GOST-7798-70") == ["DIN-933"]  # warms the cache

    snap["tables"]["standard_equivalents"] = [
        {"id": 1, "src_canonical": "GOST-5927-70", "dst_canonical": "DIN-934",
         "confidence": 100, "is_active": True}
    ]
    restore_snapshot(snap)

    assert get_standard_analogs("GOST-7798-70") == []
    assert get_standard_analogs("GOST-5927-70") == ["DIN-934"]


def test_backup_file_is_written_before_replacing(tmp_path):
    _add_equiv("GOST-7798-70", "DIN-933")
    from app.settings_backup import restore_snapshot

    backups = tmp_path / "backups"
    report = restore_snapshot(
        {"format_version": 1, "tables": {"standard_equivalents": []}},
        backup_dir=backups,
    )

    saved = list(backups.glob("settings-*.json"))
    assert len(saved) == 1
    assert report["backup_path"] == str(saved[0])

    # The backup holds what the database looked like BEFORE the replace.
    rescued = json.loads(saved[0].read_text(encoding="utf-8"))
    rows = rescued["tables"]["standard_equivalents"]
    assert [r["src_canonical"] for r in rows] == ["GOST-7798-70"]


def test_backup_can_be_restored_back(tmp_path):
    _add_equiv("GOST-7798-70", "DIN-933")
    from app.settings_backup import restore_snapshot

    backups = tmp_path / "backups"
    restore_snapshot(
        {"format_version": 1, "tables": {"standard_equivalents": []}},
        backup_dir=backups,
    )
    saved = list(backups.glob("settings-*.json"))[0]

    restore_snapshot(json.loads(saved.read_text(encoding="utf-8")))

    from app.database import get_db_session
    session = get_db_session()
    assert session.query(StandardEquivalent).count() == 1
    session.close()


def test_no_backup_dir_means_no_backup():
    _add_equiv()
    from app.settings_backup import restore_snapshot
    report = restore_snapshot({"format_version": 1, "tables": {}})
    assert report["backup_path"] is None
