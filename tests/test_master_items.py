"""Tests for MasterItem (Объединение номенклатуры).

Covers:
1. Create master group
2. Add / remove members
3. onec_guid global-uniqueness check (one guid in one group only)
4. set-primary
5. Matching — matched item in master group → master_item_id/name in result
6. Export endpoint returns correct structure
"""

import pytest
from fastapi.testclient import TestClient


# ── Test isolation fixture ──────────────────────────────────────────────────────

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


# ── Helpers ─────────────────────────────────────────────────────────────────────

def _seed_item(session, name: str, uid_1c: str | None = None) -> "InternalItem":
    from datetime import datetime, timezone
    from app.models import InternalItem
    item = InternalItem(
        name=name,
        uid_1c=uid_1c,
        is_active=True,
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )
    session.add(item)
    session.commit()
    return item


def _create_group(client, name: str, description: str = "") -> int:
    """Create a master group via API and return its id."""
    r = client.post("/api/master-items", json={"name": name, "description": description})
    assert r.status_code == 201, r.text
    return r.json()["id"]


def _add_member(client, master_id: int, guid: str, is_primary: bool = False):
    return client.post(
        f"/api/master-items/{master_id}/add",
        json={"onec_guid": guid, "is_primary": is_primary},
    )


# ── 1. Create master group ─────────────────────────────────────────────────────

class TestCreateMasterGroup:
    def test_create_via_api(self, client):
        r = client.post("/api/master-items", json={"name": "Болты М12"})
        assert r.status_code == 201
        data = r.json()
        assert data["name"] == "Болты М12"
        assert data["id"] > 0

    def test_list_shows_created_group(self, client):
        client.post("/api/master-items", json={"name": "Группа А"})
        r = client.get("/api/master-items")
        assert r.status_code == 200
        names = [g["name"] for g in r.json()]
        assert "Группа А" in names

    def test_create_empty_name_fails(self, client):
        r = client.post("/api/master-items", json={"name": ""})
        assert r.status_code == 400

    def test_detail_api(self, client):
        mid = _create_group(client, "Группа Б")
        r = client.get(f"/api/master-items/{mid}")
        assert r.status_code == 200
        assert r.json()["id"] == mid
        assert r.json()["name"] == "Группа Б"

    def test_html_list_page(self, client):
        r = client.get("/catalog/master-items")
        assert r.status_code == 200
        assert "Объединение номенклатуры" in r.text


# ── 2. Add / remove members ────────────────────────────────────────────────────

class TestMemberAddRemove:
    def test_add_member(self, client):
        from app.database import get_db_session
        session = get_db_session()
        _seed_item(session, "Болт М12x60", uid_1c="GUID-001")
        session.close()

        mid = _create_group(client, "Болты")
        r = _add_member(client, mid, "GUID-001")
        assert r.status_code == 200
        assert r.json()["ok"] is True

    def test_add_member_reflected_in_detail(self, client):
        from app.database import get_db_session
        session = get_db_session()
        _seed_item(session, "Гайка М10", uid_1c="GUID-002")
        session.close()

        mid = _create_group(client, "Гайки")
        _add_member(client, mid, "GUID-002")

        r = client.get(f"/api/master-items/{mid}")
        members = r.json()["members"]
        assert any(m["onec_guid"] == "GUID-002" for m in members)

    def test_remove_member(self, client):
        from app.database import get_db_session
        session = get_db_session()
        _seed_item(session, "Шайба 12", uid_1c="GUID-003")
        session.close()

        mid = _create_group(client, "Шайбы")
        _add_member(client, mid, "GUID-003")

        r = client.delete(f"/api/master-items/{mid}/remove/GUID-003")
        assert r.status_code == 200
        assert r.json()["ok"] is True

        # Verify removed
        r2 = client.get(f"/api/master-items/{mid}")
        members = r2.json()["members"]
        assert not any(m["onec_guid"] == "GUID-003" for m in members)

    def test_remove_nonexistent_member_returns_404(self, client):
        mid = _create_group(client, "Пустая группа")
        r = client.delete(f"/api/master-items/{mid}/remove/NO-GUID")
        assert r.status_code == 404


# ── 3. Global uniqueness of onec_guid ─────────────────────────────────────────

class TestGuidUniqueness:
    def test_guid_cannot_be_in_two_groups(self, client):
        from app.database import get_db_session
        session = get_db_session()
        _seed_item(session, "Болт М8x30 ГОСТ", uid_1c="GUID-DUP")
        session.close()

        mid1 = _create_group(client, "Группа 1")
        mid2 = _create_group(client, "Группа 2")

        r1 = _add_member(client, mid1, "GUID-DUP")
        assert r1.status_code == 200

        r2 = _add_member(client, mid2, "GUID-DUP")
        assert r2.status_code == 409, "Duplicate guid in different group should return 409"
        assert "Группа 1" in r2.json().get("error", "")

    def test_same_guid_same_group_returns_409(self, client):
        mid = _create_group(client, "Группа дубль")
        _add_member(client, mid, "GUID-X")
        r2 = _add_member(client, mid, "GUID-X")
        assert r2.status_code == 409

    def test_different_guids_in_same_group_ok(self, client):
        mid = _create_group(client, "Мульти")
        r1 = _add_member(client, mid, "GUID-A1")
        r2 = _add_member(client, mid, "GUID-A2")
        assert r1.status_code == 200
        assert r2.status_code == 200


# ── 4. Set primary ─────────────────────────────────────────────────────────────

class TestSetPrimary:
    def test_set_primary(self, client):
        mid = _create_group(client, "Группа прим")
        _add_member(client, mid, "GUID-P1")
        _add_member(client, mid, "GUID-P2")

        r = client.post(f"/api/master-items/{mid}/set-primary", json={"onec_guid": "GUID-P1"})
        assert r.status_code == 200
        assert r.json()["ok"] is True

        detail = client.get(f"/api/master-items/{mid}").json()
        for m in detail["members"]:
            if m["onec_guid"] == "GUID-P1":
                assert m["is_primary"] is True
            else:
                assert m["is_primary"] is False

    def test_set_primary_nonexistent_guid_returns_404(self, client):
        mid = _create_group(client, "Пустая")
        r = client.post(f"/api/master-items/{mid}/set-primary", json={"onec_guid": "NO-GUID"})
        assert r.status_code == 404


# ── 5. Matching returns master_item info ────────────────────────────────────────

class TestMatchingWithMasterItem:
    def test_matched_item_in_master_group_has_master_info(self, tmp_path):
        """When a match is found for an item that belongs to a master group,
        the match result should contain master_item_id and master_item_name.
        """
        import pandas as pd
        from datetime import datetime, timezone
        from app.database import get_db_session
        from app.models import InternalItem, MasterItem, MasterItemMember
        from app.matcher import add_internal_matches
        from app.match_settings import MatchSettings
        from app.matching.minhash_index import rebuild_index

        session = get_db_session()

        # Seed a catalog item with uid_1c
        item = InternalItem(
            name="Болт М12x60 ГОСТ 7798",
            uid_1c="GUID-BOLT-001",
            item_type="болт",
            size="M12x60",
            is_active=True,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )
        session.add(item)
        session.flush()

        # Seed a master group containing this item
        now = datetime.now(timezone.utc)
        mi = MasterItem(name="Болты крепежные", is_active=True, created_at=now, updated_at=now)
        session.add(mi)
        session.flush()
        session.add(MasterItemMember(
            master_item_id=mi.id,
            onec_guid="GUID-BOLT-001",
            name_original=item.name,
            is_primary=True,
            created_at=now,
        ))
        session.commit()
        session.close()

        rebuild_index([item], num_perm=64, threshold=0.05, ngram_n=4, use_type_buckets=False)

        settings = MatchSettings(
            enable_minhash=True,
            lsh_threshold=0.05,
            num_perm=64,
            minhash_top_k=10,
            ngram_n=4,
            use_type_buckets=False,
            min_candidates_before_fallback=1,
            auto_apply_enabled=True,
            auto_apply_jaccard_threshold=0.0,
            min_display_score=0,
        )
        df = pd.DataFrame([{
            "name": "Болт М12x60 ГОСТ 7798",
            "name_raw": "Болт М12x60 ГОСТ 7798",
            "item_type": "болт",
            "size": "M12x60",
            "gost": "", "iso": "", "din": "",
            "diameter": "", "length": "", "strength": "", "coating": "",
        }])

        _, results = add_internal_matches(df, settings=settings)
        assert len(results) == 1
        r = results[0]

        assert r.get("master_item_id") == mi.id, (
            f"Expected master_item_id={mi.id}, got {r.get('master_item_id')}"
        )
        assert r.get("master_item_name") == "Болты крепежные", (
            f"Expected master_item_name='Болты крепежные', got {r.get('master_item_name')}"
        )

    def test_unmatched_item_has_no_master_info(self, tmp_path):
        """Item not in any master group → master_item_id/name are None."""
        import pandas as pd
        from datetime import datetime, timezone
        from app.database import get_db_session
        from app.models import InternalItem
        from app.matcher import add_internal_matches
        from app.match_settings import MatchSettings
        from app.matching.minhash_index import rebuild_index

        session = get_db_session()
        item = InternalItem(
            name="Шайба 10 ГОСТ 11371",
            uid_1c="GUID-WASHER-001",
            item_type="шайба",
            is_active=True,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )
        session.add(item)
        session.commit()
        session.close()

        rebuild_index([item], num_perm=64, threshold=0.05, ngram_n=4, use_type_buckets=False)

        settings = MatchSettings(
            enable_minhash=True, lsh_threshold=0.05, num_perm=64, minhash_top_k=10,
            ngram_n=4, use_type_buckets=False, min_candidates_before_fallback=1,
            auto_apply_enabled=True, auto_apply_jaccard_threshold=0.0, min_display_score=0,
        )
        df = pd.DataFrame([{
            "name": "Шайба 10 ГОСТ 11371", "name_raw": "Шайба 10 ГОСТ 11371",
            "item_type": "шайба", "size": "",
            "gost": "", "iso": "", "din": "",
            "diameter": "", "length": "", "strength": "", "coating": "",
        }])
        _, results = add_internal_matches(df, settings=settings)
        r = results[0]
        assert r.get("master_item_id") is None
        assert r.get("master_item_name") is None


# ── 6. Export endpoint ─────────────────────────────────────────────────────────

class TestExport:
    def test_export_returns_correct_structure(self, client):
        from app.database import get_db_session
        session = get_db_session()
        _seed_item(session, "Болт М6x20 DIN 933", uid_1c="GUID-EXP-1")
        _seed_item(session, "Болт М6x20 ГОСТ 7798", uid_1c="GUID-EXP-2")
        session.close()

        mid = _create_group(client, "Болты М6x20 (все стандарты)")
        _add_member(client, mid, "GUID-EXP-1", is_primary=True)
        _add_member(client, mid, "GUID-EXP-2")

        r = client.get("/api/master-items/export")
        assert r.status_code == 200
        data = r.json()
        assert isinstance(data, list)
        group = next((g for g in data if g["master_id"] == mid), None)
        assert group is not None
        assert group["master_name"] == "Болты М6x20 (все стандарты)"
        assert len(group["members"]) == 2
        guids = {m["onec_guid"] for m in group["members"]}
        assert "GUID-EXP-1" in guids
        assert "GUID-EXP-2" in guids
        # Primary member should be marked
        primary = next(m for m in group["members"] if m["onec_guid"] == "GUID-EXP-1")
        assert primary["is_primary"] is True

    def test_export_empty_when_no_groups(self, client):
        r = client.get("/api/master-items/export")
        assert r.status_code == 200
        assert r.json() == []

    def test_member_count_in_list(self, client):
        mid = _create_group(client, "Группа счёт")
        _add_member(client, mid, "GUID-C1")
        _add_member(client, mid, "GUID-C2")
        _add_member(client, mid, "GUID-C3")

        r = client.get("/api/master-items")
        group = next(g for g in r.json() if g["id"] == mid)
        assert group["member_count"] == 3
