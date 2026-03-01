"""Tests for /catalog/duplicates — automatic duplicate/analog analysis.

Strict rules:
  - Items without type_norm OR size_key are excluded from all groups.
  - Duplicates: composite key (type_norm, size_key, canonical_name_key) must match;
    if both standards are filled and differ → skip (analog, not duplicate).
  - Analogs: (type_norm, size_key) must match AND standards are equivalent
    in standard_equivalents table.

Covers:
1.  canonical_name_key normalization (case, ё→е, Cyrillic-х→x, мм-strip)
2.  Items with same type+size+canonical → grouped as duplicates
3.  Missing type → excluded from grouping
4.  Missing size → excluded from grouping
5.  Type mismatch → not grouped
6.  Size mismatch → not grouped (core spec: DIN-933 M8x25 ≠ DIN-933 M8x70)
7.  Same standard, same type, different size → NOT grouped
8.  Different standards, same type+size, standards equivalent → analog group
9.  Different standards, same type, different size → NOT grouped (spec)
10. Different standards, same type+size, standards NOT equivalent → not grouped
11. Duplicate with both standards differ → not a duplicate
12. Analog detection is bidirectional (seed A→B, find B→A items)
13. Parent selection: lowest folder_priority wins
14. Parent selection: "основн" in path preferred when no priority
15. Parent selection: priority beats "основн"
16. Parent selection: uid_1c as deterministic tiebreak
17. include_duplicates=False → no duplicate groups
18. include_analogs=False → no analog groups
19. Mixed group: duplicate + analog edges
20. Determinism across calls
21. HTML form page loads (GET)
22. POST computes and renders
23. POST text filter
24. CSV export structure and content
25. diameter+length fallback for size_key
"""

import pytest
from datetime import datetime, timezone
from fastapi.testclient import TestClient


# ── Test isolation fixture ───────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def _set_dirs(tmp_path, monkeypatch):
    upload_dir = tmp_path / "uploads"
    cache_dir  = tmp_path / "cache"
    monkeypatch.setenv("OTDELZAKUP_UPLOAD_DIR", str(upload_dir))
    monkeypatch.setenv("OTDELZAKUP_CACHE_DIR",  str(cache_dir))
    import app.cache as cache_mod
    cache_mod.UPLOAD_DIR = upload_dir
    cache_mod.CACHE_DIR  = cache_dir

    db_path = tmp_path / "test.db"
    monkeypatch.setenv("OTDELZAKUP_DB_PATH", str(db_path))
    import app.database as db_mod
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    db_mod.DB_PATH = db_path
    db_mod.engine = create_engine(f"sqlite:///{db_path}", connect_args={"check_same_thread": False})
    db_mod.SessionLocal = sessionmaker(bind=db_mod.engine, autoflush=False, expire_on_commit=False)
    db_mod.init_db()


@pytest.fixture()
def client():
    from app.main import app
    return TestClient(app, raise_server_exceptions=True)


# ── Helpers ──────────────────────────────────────────────────────────────────

def _seed_item(
    session,
    name: str,
    uid_1c: str | None = None,
    item_type: str | None = None,
    size: str | None = None,
    diameter: str | None = None,
    length: str | None = None,
    standard_key: str | None = None,
    folder_path: str | None = None,
    folder_priority: int | None = None,
):
    from app.models import InternalItem
    now = datetime.now(timezone.utc)
    item = InternalItem(
        name=name,
        uid_1c=uid_1c,
        item_type=item_type,
        size=size,
        diameter=diameter,
        length=length,
        standard_key=standard_key,
        folder_path=folder_path,
        folder_priority=folder_priority,
        is_active=True,
        created_at=now,
        updated_at=now,
    )
    session.add(item)
    session.commit()
    return item


def _seed_equiv(session, src: str, dst: str, confidence: int = 100):
    from app.models import StandardEquivalent
    now = datetime.now(timezone.utc)
    se = StandardEquivalent(
        src_canonical=src, dst_canonical=dst,
        confidence=confidence, is_active=True,
        created_at=now, updated_at=now,
    )
    session.add(se)
    session.commit()
    return se


def _all_ids(group: dict) -> set[int]:
    return {group["parent"].id} | {c.id for c in group["children"]}


# ── 1. canonical_name_key normalization ─────────────────────────────────────

class TestCanonicalNameKey:
    def test_lowercase(self):
        from app.catalog_duplicates import canonical_name_key
        assert canonical_name_key("Болт М12") == canonical_name_key("болт м12")

    def test_yo_to_ye(self):
        from app.catalog_duplicates import canonical_name_key
        assert canonical_name_key("Шёлковый") == canonical_name_key("Шелковый")

    def test_cyrillic_x_to_latin_x(self):
        from app.catalog_duplicates import canonical_name_key
        assert canonical_name_key("М12х60") == canonical_name_key("м12x60")

    def test_unicode_times_to_x(self):
        from app.catalog_duplicates import canonical_name_key
        assert canonical_name_key("10\u00d720") == canonical_name_key("10x20")

    def test_decimal_comma_to_dot(self):
        from app.catalog_duplicates import canonical_name_key
        assert canonical_name_key("4,8") == canonical_name_key("4.8")

    def test_mm_stopword_stripped(self):
        from app.catalog_duplicates import canonical_name_key
        assert canonical_name_key("Болт М10 мм") == canonical_name_key("Болт М10")

    def test_empty_returns_empty(self):
        from app.catalog_duplicates import canonical_name_key
        assert canonical_name_key("") == ""
        assert canonical_name_key(None) == ""  # type: ignore[arg-type]


# ── 2. Duplicate detection (strict: type+size required) ──────────────────────

class TestDuplicateDetection:
    def test_same_type_size_canonical_grouped(self):
        from app.database import get_db_session
        from app.catalog_duplicates import compute_duplicate_groups
        session = get_db_session()
        a = _seed_item(session, "Болт DIN 933 M8x25", uid_1c="G1",
                       item_type="болт", size="M8x25", standard_key="DIN-933")
        b = _seed_item(session, "Болт DIN 933 M8x25", uid_1c="G2",
                       item_type="болт", size="M8x25", standard_key="DIN-933")
        session.close()

        groups = compute_duplicate_groups(include_duplicates=True, include_analogs=False)
        assert len(groups) == 1
        assert _all_ids(groups[0]) == {a.id, b.id}

    def test_case_yo_normalized_duplicate(self):
        from app.database import get_db_session
        from app.catalog_duplicates import compute_duplicate_groups
        session = get_db_session()
        a = _seed_item(session, "Болт DIN 933 M8x25", uid_1c="G3",
                       item_type="болт", size="M8x25")
        b = _seed_item(session, "болт din 933 m8x25", uid_1c="G4",
                       item_type="болт", size="M8x25")
        session.close()

        groups = compute_duplicate_groups(include_duplicates=True, include_analogs=False)
        assert len(groups) == 1
        assert _all_ids(groups[0]) == {a.id, b.id}

    def test_missing_type_excluded(self):
        """Item without item_type is excluded from duplicate grouping."""
        from app.database import get_db_session
        from app.catalog_duplicates import compute_duplicate_groups
        session = get_db_session()
        _seed_item(session, "Болт M8x25", uid_1c="G5",
                   item_type=None, size="M8x25")   # no type
        _seed_item(session, "Болт M8x25", uid_1c="G6",
                   item_type="болт", size="M8x25")
        session.close()

        # The item without type cannot form a group
        groups = compute_duplicate_groups(include_duplicates=True, include_analogs=False)
        assert len(groups) == 0

    def test_missing_size_excluded(self):
        """Item without size is excluded from duplicate grouping."""
        from app.database import get_db_session
        from app.catalog_duplicates import compute_duplicate_groups
        session = get_db_session()
        _seed_item(session, "Болт DIN 933", uid_1c="G7",
                   item_type="болт", size=None)    # no size
        _seed_item(session, "Болт DIN 933", uid_1c="G8",
                   item_type="болт", size=None)    # no size
        session.close()

        groups = compute_duplicate_groups(include_duplicates=True, include_analogs=False)
        assert len(groups) == 0

    def test_type_mismatch_not_grouped(self):
        from app.database import get_db_session
        from app.catalog_duplicates import compute_duplicate_groups
        session = get_db_session()
        _seed_item(session, "Крепёж M8x25", uid_1c="G9",
                   item_type="болт", size="M8x25")
        _seed_item(session, "Крепёж M8x25", uid_1c="G10",
                   item_type="гайка", size="M8x25")
        session.close()

        groups = compute_duplicate_groups(include_duplicates=True, include_analogs=False)
        assert len(groups) == 0

    def test_size_mismatch_not_grouped(self):
        """Core spec: DIN-933 M8x25 and DIN-933 M8x70 must NOT be in same group."""
        from app.database import get_db_session
        from app.catalog_duplicates import compute_duplicate_groups
        session = get_db_session()
        _seed_item(session, "Болт DIN 933", uid_1c="G11",
                   item_type="болт", size="M8x25", standard_key="DIN-933")
        _seed_item(session, "Болт DIN 933", uid_1c="G12",
                   item_type="болт", size="M8x70", standard_key="DIN-933")
        session.close()

        groups = compute_duplicate_groups(include_duplicates=True, include_analogs=True)
        assert len(groups) == 0, \
            "DIN-933 M8x25 and DIN-933 M8x70 must NOT be in the same group"

    def test_different_standards_not_duplicate(self):
        """Same name+type+size but different (non-equivalent) standards → not a duplicate."""
        from app.database import get_db_session
        from app.catalog_duplicates import compute_duplicate_groups
        session = get_db_session()
        _seed_item(session, "Болт M8x25", uid_1c="G13",
                   item_type="болт", size="M8x25", standard_key="DIN-933")
        _seed_item(session, "Болт M8x25", uid_1c="G14",
                   item_type="болт", size="M8x25", standard_key="GOST-7798-70")
        # No StandardEquivalent seeded → they're NOT linked as analogs either
        session.close()

        groups = compute_duplicate_groups(include_duplicates=True, include_analogs=False)
        assert len(groups) == 0

    def test_three_same_items_in_one_group(self):
        from app.database import get_db_session
        from app.catalog_duplicates import compute_duplicate_groups
        session = get_db_session()
        a = _seed_item(session, "Шайба ГОСТ 11371 M12", uid_1c="G15",
                       item_type="шайба", size="M12")
        b = _seed_item(session, "Шайба ГОСТ 11371 M12", uid_1c="G16",
                       item_type="шайба", size="M12")
        c = _seed_item(session, "Шайба ГОСТ 11371 M12", uid_1c="G17",
                       item_type="шайба", size="M12")
        session.close()

        groups = compute_duplicate_groups(include_duplicates=True, include_analogs=False)
        assert len(groups) == 1
        assert groups[0]["size"] == 3
        assert _all_ids(groups[0]) == {a.id, b.id, c.id}


# ── 3. Analog detection ──────────────────────────────────────────────────────

class TestAnalogDetection:
    def test_equivalent_standards_same_size_grouped(self):
        """Core spec: DIN-933 M8x25 and ISO-4017 M8x25 → analog group."""
        from app.database import get_db_session
        from app.catalog_duplicates import compute_duplicate_groups
        session = get_db_session()
        a = _seed_item(session, "Болт DIN 933 M8x25", uid_1c="A1",
                       item_type="болт", size="M8x25", standard_key="DIN-933")
        b = _seed_item(session, "Болт ISO 4017 M8x25", uid_1c="A2",
                       item_type="болт", size="M8x25", standard_key="ISO-4017")
        _seed_equiv(session, "DIN-933", "ISO-4017")
        session.close()

        groups = compute_duplicate_groups(include_duplicates=False, include_analogs=True)
        assert len(groups) == 1
        assert _all_ids(groups[0]) == {a.id, b.id}

    def test_equivalent_standards_different_size_not_grouped(self):
        """Core spec: DIN-933 M8x25 and ISO-4017 M8x70 must NOT be in same group."""
        from app.database import get_db_session
        from app.catalog_duplicates import compute_duplicate_groups
        session = get_db_session()
        _seed_item(session, "Болт DIN 933 M8x25", uid_1c="A3",
                   item_type="болт", size="M8x25", standard_key="DIN-933")
        _seed_item(session, "Болт ISO 4017 M8x70", uid_1c="A4",
                   item_type="болт", size="M8x70", standard_key="ISO-4017")
        _seed_equiv(session, "DIN-933", "ISO-4017")
        session.close()

        groups = compute_duplicate_groups(include_duplicates=False, include_analogs=True)
        assert len(groups) == 0, \
            "DIN-933 M8x25 and ISO-4017 M8x70 must NOT be grouped"

    def test_three_standards_same_size_all_grouped(self):
        """DIN-933 ↔ ISO-4017 ↔ GOST-7798-70 M8x25 — all three in one group."""
        from app.database import get_db_session
        from app.catalog_duplicates import compute_duplicate_groups
        session = get_db_session()
        a = _seed_item(session, "Болт DIN 933 M8x25",   uid_1c="A5",
                       item_type="болт", size="M8x25", standard_key="DIN-933")
        b = _seed_item(session, "Болт ISO 4017 M8x25",  uid_1c="A6",
                       item_type="болт", size="M8x25", standard_key="ISO-4017")
        c = _seed_item(session, "Болт ГОСТ 7798 M8x25", uid_1c="A7",
                       item_type="болт", size="M8x25", standard_key="GOST-7798-70")
        _seed_equiv(session, "DIN-933", "ISO-4017")
        _seed_equiv(session, "DIN-933", "GOST-7798-70")
        session.close()

        groups = compute_duplicate_groups(include_duplicates=False, include_analogs=True)
        assert len(groups) == 1
        assert _all_ids(groups[0]) == {a.id, b.id, c.id}

    def test_analog_reason_label(self):
        from app.database import get_db_session
        from app.catalog_duplicates import compute_duplicate_groups
        session = get_db_session()
        _seed_item(session, "Болт DIN 933 M12x60", uid_1c="A8",
                   item_type="болт", size="M12x60", standard_key="DIN-933")
        _seed_item(session, "Болт ISO 4017 M12x60", uid_1c="A9",
                   item_type="болт", size="M12x60", standard_key="ISO-4017")
        _seed_equiv(session, "DIN-933", "ISO-4017")
        session.close()

        groups = compute_duplicate_groups(include_duplicates=False, include_analogs=True)
        assert groups[0]["child_info"][0]["reason"] == "analog"
        detail = groups[0]["child_info"][0]["detail"]
        assert "DIN-933" in detail or "ISO-4017" in detail

    def test_analog_type_mismatch_not_grouped(self):
        from app.database import get_db_session
        from app.catalog_duplicates import compute_duplicate_groups
        session = get_db_session()
        _seed_item(session, "Болт DIN 933 M12x60", uid_1c="A10",
                   item_type="болт", size="M12x60", standard_key="DIN-933")
        _seed_item(session, "Гайка ISO 4032 M12",  uid_1c="A11",
                   item_type="гайка", size="M12x60", standard_key="ISO-4017")
        _seed_equiv(session, "DIN-933", "ISO-4017")
        session.close()

        groups = compute_duplicate_groups(include_duplicates=False, include_analogs=True)
        assert len(groups) == 0

    def test_analog_missing_standard_excluded(self):
        """Item without standard_key cannot form an analog group."""
        from app.database import get_db_session
        from app.catalog_duplicates import compute_duplicate_groups
        session = get_db_session()
        _seed_item(session, "Болт M8x25", uid_1c="A12",
                   item_type="болт", size="M8x25", standard_key=None)
        _seed_item(session, "Болт DIN M8x25", uid_1c="A13",
                   item_type="болт", size="M8x25", standard_key="DIN-933")
        _seed_equiv(session, "DIN-933", "ISO-4017")
        session.close()

        groups = compute_duplicate_groups(include_duplicates=False, include_analogs=True)
        assert len(groups) == 0

    def test_bidirectional_equiv(self):
        """Seeding A→B must find items where one has B and the other has A."""
        from app.database import get_db_session
        from app.catalog_duplicates import compute_duplicate_groups
        session = get_db_session()
        a = _seed_item(session, "X ISO", uid_1c="A14",
                       item_type="болт", size="M10x30", standard_key="ISO-4017")
        b = _seed_item(session, "X DIN", uid_1c="A15",
                       item_type="болт", size="M10x30", standard_key="DIN-933")
        # Seed reverse direction only
        _seed_equiv(session, "ISO-4017", "DIN-933")
        session.close()

        groups = compute_duplicate_groups(include_duplicates=False, include_analogs=True)
        assert len(groups) == 1
        assert _all_ids(groups[0]) == {a.id, b.id}

    def test_no_equivs_no_analog_groups(self):
        from app.database import get_db_session
        from app.catalog_duplicates import compute_duplicate_groups
        session = get_db_session()
        _seed_item(session, "X DIN",  uid_1c="A16",
                   item_type="болт", size="M8x25", standard_key="DIN-933")
        _seed_item(session, "X ISO",  uid_1c="A17",
                   item_type="болт", size="M8x25", standard_key="ISO-4017")
        # No StandardEquivalent rows → no analogs
        session.close()

        groups = compute_duplicate_groups(include_duplicates=False, include_analogs=True)
        assert len(groups) == 0


# ── 4. Size key: diameter+length fallback ────────────────────────────────────

class TestSizeKeyFallback:
    def test_diameter_and_length_used_when_size_empty(self):
        """If size field is empty, diameter+length should provide the size_key."""
        from app.database import get_db_session
        from app.catalog_duplicates import compute_duplicate_groups
        session = get_db_session()
        # Both have diameter+length instead of size
        a = _seed_item(session, "Гайка DIN 934 M10", uid_1c="SZ1",
                       item_type="гайка", size=None, diameter="M10", length="8",
                       standard_key="DIN-934")
        b = _seed_item(session, "Гайка DIN 934 M10", uid_1c="SZ2",
                       item_type="гайка", size=None, diameter="M10", length="8",
                       standard_key="DIN-934")
        session.close()

        groups = compute_duplicate_groups(include_duplicates=True, include_analogs=False)
        assert len(groups) == 1
        assert _all_ids(groups[0]) == {a.id, b.id}

    def test_only_diameter_used_as_size_key(self):
        """diameter only (no length) → size_key from diameter alone."""
        from app.database import get_db_session
        from app.catalog_duplicates import compute_duplicate_groups
        session = get_db_session()
        a = _seed_item(session, "Шайба M12", uid_1c="SZ3",
                       item_type="шайба", size=None, diameter="M12")
        b = _seed_item(session, "Шайба M12", uid_1c="SZ4",
                       item_type="шайба", size=None, diameter="M12")
        session.close()

        groups = compute_duplicate_groups(include_duplicates=True, include_analogs=False)
        assert len(groups) == 1

    def test_size_field_takes_priority_over_diameter(self):
        """If size is filled, diameter+length are ignored for size_key."""
        from app.catalog_duplicates import _item_size_key
        from app.models import InternalItem
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc)
        item = InternalItem(name="X", size="M8x25", diameter="M10", length="50",
                            is_active=True, created_at=now, updated_at=now)
        key = _item_size_key(item)
        # Should use size="M8x25" → "8x25", NOT diameter+length
        assert key == "8x25"


# ── 5. Parent selection ──────────────────────────────────────────────────────

class TestParentSelection:
    def test_lower_folder_priority_is_parent(self):
        from app.database import get_db_session
        from app.catalog_duplicates import compute_duplicate_groups
        session = get_db_session()
        a = _seed_item(session, "Болт DIN 933 M10x30", uid_1c="P1",
                       item_type="болт", size="M10x30", folder_priority=1)
        b = _seed_item(session, "Болт DIN 933 M10x30", uid_1c="P2",
                       item_type="болт", size="M10x30", folder_priority=5)
        session.close()

        groups = compute_duplicate_groups(include_duplicates=True, include_analogs=False)
        assert len(groups) == 1
        assert groups[0]["parent"].id == a.id

    def test_osnov_folder_preferred_over_null_priority(self):
        from app.database import get_db_session
        from app.catalog_duplicates import compute_duplicate_groups
        session = get_db_session()
        a = _seed_item(session, "Болт DIN 933 M8x20", uid_1c="P3",
                       item_type="болт", size="M8x20",
                       folder_path="/Основные болты/", folder_priority=None)
        b = _seed_item(session, "Болт DIN 933 M8x20", uid_1c="P4",
                       item_type="болт", size="M8x20",
                       folder_path="/Дублирующие/", folder_priority=None)
        session.close()

        groups = compute_duplicate_groups(include_duplicates=True, include_analogs=False)
        assert len(groups) == 1
        assert groups[0]["parent"].id == a.id

    def test_explicit_priority_beats_osnov_folder(self):
        from app.database import get_db_session
        from app.catalog_duplicates import compute_duplicate_groups
        session = get_db_session()
        a = _seed_item(session, "Шайба M10", uid_1c="P5",
                       item_type="шайба", size="M10",
                       folder_path="/Обычные/", folder_priority=1)
        b = _seed_item(session, "Шайба M10", uid_1c="P6",
                       item_type="шайба", size="M10",
                       folder_path="/Основные/", folder_priority=None)
        session.close()

        groups = compute_duplicate_groups(include_duplicates=True, include_analogs=False)
        assert len(groups) == 1
        assert groups[0]["parent"].id == a.id  # explicit priority=1 < 999999

    def test_uid_tiebreak_deterministic(self):
        """All else equal, uid_1c alphabetically determines parent."""
        from app.database import get_db_session
        from app.catalog_duplicates import compute_duplicate_groups
        session = get_db_session()
        a = _seed_item(session, "Гайка M6", uid_1c="AAA-FIRST",
                       item_type="гайка", size="M6")
        b = _seed_item(session, "Гайка M6", uid_1c="ZZZ-LAST",
                       item_type="гайка", size="M6")
        session.close()

        groups = compute_duplicate_groups(include_duplicates=True, include_analogs=False)
        assert len(groups) == 1
        assert groups[0]["parent"].uid_1c == "AAA-FIRST"


# ── 6. Flags ─────────────────────────────────────────────────────────────────

class TestFlags:
    def test_no_duplicates_when_flag_off(self):
        from app.database import get_db_session
        from app.catalog_duplicates import compute_duplicate_groups
        session = get_db_session()
        _seed_item(session, "Болт DIN 933 M8x25", uid_1c="F1",
                   item_type="болт", size="M8x25", standard_key="DIN-933")
        _seed_item(session, "Болт DIN 933 M8x25", uid_1c="F2",
                   item_type="болт", size="M8x25", standard_key="DIN-933")
        session.close()

        groups = compute_duplicate_groups(include_duplicates=False, include_analogs=False)
        assert len(groups) == 0

    def test_no_analogs_when_flag_off(self):
        from app.database import get_db_session
        from app.catalog_duplicates import compute_duplicate_groups
        session = get_db_session()
        _seed_item(session, "Болт DIN 933 M8x25", uid_1c="F3",
                   item_type="болт", size="M8x25", standard_key="DIN-933")
        _seed_item(session, "Болт ISO 4017 M8x25", uid_1c="F4",
                   item_type="болт", size="M8x25", standard_key="ISO-4017")
        _seed_equiv(session, "DIN-933", "ISO-4017")
        session.close()

        # Different names → no duplicate; analog flag off → no analog
        groups = compute_duplicate_groups(include_duplicates=True, include_analogs=False)
        assert len(groups) == 0

    def test_mixed_duplicate_and_analog_in_one_group(self):
        """a≡b (duplicate) and b~c (analog) → a, b, c all in one component."""
        from app.database import get_db_session
        from app.catalog_duplicates import compute_duplicate_groups
        session = get_db_session()
        a = _seed_item(session, "Болт DIN 933 M8x25", uid_1c="F5",
                       item_type="болт", size="M8x25", standard_key="DIN-933")
        b = _seed_item(session, "Болт DIN 933 M8x25", uid_1c="F6",
                       item_type="болт", size="M8x25", standard_key="DIN-933")
        c = _seed_item(session, "Болт ISO 4017 M8x25", uid_1c="F7",
                       item_type="болт", size="M8x25", standard_key="ISO-4017")
        _seed_equiv(session, "DIN-933", "ISO-4017")
        session.close()

        groups = compute_duplicate_groups(include_duplicates=True, include_analogs=True)
        assert len(groups) == 1
        assert _all_ids(groups[0]) == {a.id, b.id, c.id}


# ── 7. Determinism ───────────────────────────────────────────────────────────

class TestDeterminism:
    def test_same_result_across_calls(self):
        from app.database import get_db_session
        from app.catalog_duplicates import compute_duplicate_groups
        session = get_db_session()
        for i in range(5):
            _seed_item(session, "Болт DIN 933 M10x30", uid_1c=f"D{i}",
                       item_type="болт", size="M10x30", standard_key="DIN-933")
        session.close()

        r1 = compute_duplicate_groups(include_duplicates=True, include_analogs=False)
        r2 = compute_duplicate_groups(include_duplicates=True, include_analogs=False)
        assert len(r1) == len(r2)
        for g1, g2 in zip(r1, r2):
            assert g1["parent"].id == g2["parent"].id
            assert [c.id for c in g1["children"]] == [c.id for c in g2["children"]]


# ── 8. Route / HTTP tests ────────────────────────────────────────────────────

class TestRoutes:
    def test_get_form_page(self, client):
        r = client.get("/catalog/duplicates")
        assert r.status_code == 200
        assert "дубликат" in r.text.lower() or "аналог" in r.text.lower()

    def test_hint_text_shown(self, client):
        r = client.get("/catalog/duplicates")
        assert "распознан" in r.text.lower() or "размер" in r.text.lower()

    def test_post_no_groups_empty_catalog(self, client):
        r = client.post("/catalog/duplicates", data={
            "include_duplicates": "1",
            "include_analogs": "1",
            "min_size": "2",
            "q": "",
        })
        assert r.status_code == 200
        assert "не найдено" in r.text.lower() or "групп" in r.text.lower()

    def test_post_computes_duplicate_groups(self, client):
        from app.database import get_db_session
        session = get_db_session()
        _seed_item(session, "Болт DIN 933 M8x25 тест", uid_1c="R1",
                   item_type="болт", size="M8x25", standard_key="DIN-933")
        _seed_item(session, "Болт DIN 933 M8x25 тест", uid_1c="R2",
                   item_type="болт", size="M8x25", standard_key="DIN-933")
        session.close()

        r = client.post("/catalog/duplicates", data={
            "include_duplicates": "1",
            "include_analogs": "",
            "min_size": "2",
            "q": "",
        })
        assert r.status_code == 200
        assert "Болт DIN 933 M8x25 тест" in r.text

    def test_post_text_filter(self, client):
        from app.database import get_db_session
        session = get_db_session()
        _seed_item(session, "Болт тестовый M8x25", uid_1c="R3",
                   item_type="болт", size="M8x25")
        _seed_item(session, "Болт тестовый M8x25", uid_1c="R4",
                   item_type="болт", size="M8x25")
        _seed_item(session, "Гайка прочная M10", uid_1c="R5",
                   item_type="гайка", size="M10")
        _seed_item(session, "Гайка прочная M10", uid_1c="R6",
                   item_type="гайка", size="M10")
        session.close()

        r = client.post("/catalog/duplicates", data={
            "include_duplicates": "1",
            "include_analogs": "",
            "min_size": "2",
            "q": "болт",
        })
        assert r.status_code == 200
        assert "Болт тестовый M8x25" in r.text
        assert "Гайка прочная M10" not in r.text

    def test_csv_export_structure(self, client):
        from app.database import get_db_session
        session = get_db_session()
        _seed_item(session, "Шайба M12 экспорт", uid_1c="E1",
                   item_type="шайба", size="M12")
        _seed_item(session, "Шайба M12 экспорт", uid_1c="E2",
                   item_type="шайба", size="M12")
        session.close()

        r = client.get("/api/catalog/duplicates/export?include_duplicates=true&include_analogs=false")
        assert r.status_code == 200
        assert "text/csv" in r.headers["content-type"]
        lines = r.content.decode("utf-8-sig").strip().splitlines()
        assert len(lines) >= 3  # header + parent row + child row
        assert "group_num" in lines[0]
        assert "reason" in lines[0]
        assert "parent" in lines[1]
        assert "child" in lines[2]

    def test_csv_export_empty(self, client):
        r = client.get("/api/catalog/duplicates/export?include_duplicates=true&include_analogs=false")
        assert r.status_code == 200
        lines = r.content.decode("utf-8-sig").strip().splitlines()
        assert len(lines) == 1  # header only
