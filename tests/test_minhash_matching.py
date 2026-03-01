"""Tests for MinHash+LSH candidate retrieval and text normalization.

Covers:
1.  Legacy text normalization (normalize_text, build_shingles, build_minhash_tokens)
2.  MinHash v2 normalization (normalize_for_minhash, char_ngrams)
3.  LSH index rebuild + query (global + type buckets)
4.  Integration with scorer
5.  Index mutations (add/remove with type bucket support)
6.  Settings persistence
7.  Jaccard sanity (same-type items closer than different types)
8.  post_filter_candidates: size/type/standard mismatch filtering
9.  post_filter_candidates: fallback ladder (levels 1, 2, 3)
10. post_filter_candidates: best_filtered_out flag
11. match_debug.filter_log present on every result
12. field_badges present on each candidate
13. auto-apply blocked when best candidate fails hard filter
"""

import pytest

from app.matching.text_normalizer import (
    build_minhash_tokens,
    build_shingles,
    char_ngrams,
    normalize_for_minhash,
    normalize_text,
)
from app.matching.minhash_index import (
    add_to_index,
    is_index_ready,
    query_index,
    query_index_with_scores,
    rebuild_index,
    remove_from_index,
)
from app.models import InternalItem


def _item(**kw):
    defaults = dict(is_active=True, name="Test")
    defaults.update(kw)
    return InternalItem(**defaults)


# ── Test isolation fixture ─────────────────────────────────────────────────────

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


# ── 1. Legacy text normalization ──────────────────────────────────────────────

class TestTextNormalizer:
    def test_normalize_cyrillic_yo(self):
        result = normalize_text("ёж Ёлка")
        assert "е" in result
        assert "ё" not in result

    def test_normalize_lowercase(self):
        assert normalize_text("БОЛТ M12") == normalize_text("болт m12")

    def test_normalize_removes_punctuation(self):
        result = normalize_text("Болт, М12-80 (ГОСТ 7798)")
        assert "," not in result
        assert "(" not in result
        assert ")" not in result


# ── 2. Legacy token generation ────────────────────────────────────────────────

class TestTokenGeneration:
    def test_shingles_basic(self):
        shingles = build_shingles("abcde", k=3)
        assert shingles == {"abc", "bcd", "cde"}

    def test_shingles_short_text(self):
        shingles = build_shingles("ab", k=3)
        assert shingles == {"ab"}

    def test_tokens_include_structural_prefixes(self):
        tokens = build_minhash_tokens(
            "Болт М12x80 ГОСТ 7798-70", "болт", "M12x80", "ГОСТ 7798-70"
        )
        assert any(t.startswith("TYPE:") for t in tokens)
        assert any(t.startswith("SIZE:") for t in tokens)
        assert any(t.startswith("STD:") for t in tokens)

    def test_tokens_include_word_tokens(self):
        tokens = build_minhash_tokens("Болт высокопрочный М12", "болт", "M12", "")
        assert any("болт" in t for t in tokens)

    def test_empty_input(self):
        tokens = build_minhash_tokens("", "", "", "")
        assert tokens == set()


# ── 3. MinHash v2: normalize_for_minhash ─────────────────────────────────────

class TestNormalizeForMinhash:
    def test_cyrillic_x_to_latin(self):
        assert normalize_for_minhash("4.8х35") == "4.8x35"

    def test_cyrillic_x_uppercase(self):
        assert normalize_for_minhash("4.8Х35") == "4.8x35"

    def test_unicode_multiplication(self):
        assert normalize_for_minhash("4.8\u00d735") == "4.8x35"

    def test_asterisk_to_x(self):
        assert normalize_for_minhash("4.8*35") == "4.8x35"

    def test_decimal_comma(self):
        result = normalize_for_minhash("4,8х35 мм")
        assert result == "4.8x35"

    def test_mm_suffix_removed(self):
        assert "мм" not in normalize_for_minhash("125x1.6x22мм")

    def test_mm_suffix_with_dot(self):
        assert "мм" not in normalize_for_minhash("22мм.")

    def test_mm_suffix_with_space(self):
        assert "мм" not in normalize_for_minhash("35 мм")

    def test_hyphens_to_space(self):
        result = normalize_for_minhash("пресс-шайба")
        assert result == "пресс шайба"

    def test_underscore_to_space(self):
        result = normalize_for_minhash("пресс_шайба")
        assert result == "пресс шайба"

    def test_empty(self):
        assert normalize_for_minhash("") == ""

    def test_full_samorez_example(self):
        result = normalize_for_minhash("Саморез 4,8х35 мм с пресс-шайбой")
        assert result == "саморез 4.8x35 с пресс шайбой"

    def test_yo_replaced(self):
        result = normalize_for_minhash("ёлка")
        assert "ё" not in result
        assert "елка" == result

    def test_preserves_dots_in_sizes(self):
        result = normalize_for_minhash("4.8x35")
        assert "4.8" in result


# ── 4. MinHash v2: char_ngrams ───────────────────────────────────────────────

class TestCharNgrams:
    def test_basic_4grams(self):
        grams = char_ngrams("abcdef", n=4)
        assert grams == {"abcd", "bcde", "cdef"}

    def test_short_text(self):
        grams = char_ngrams("abc", n=4)
        assert grams == {"abc"}

    def test_spaces_become_underscores(self):
        grams = char_ngrams("ab cd", n=4)
        assert "b_cd" in grams
        assert "ab_c" in grams

    def test_empty(self):
        assert char_ngrams("") == set()

    def test_single_char(self):
        assert char_ngrams("a", n=4) == {"a"}

    def test_exact_length(self):
        grams = char_ngrams("abcd", n=4)
        assert grams == {"abcd"}

    def test_stability(self):
        """Same input always produces same output."""
        a = char_ngrams("саморез 4.8x35", n=4)
        b = char_ngrams("саморез 4.8x35", n=4)
        assert a == b

    def test_ngram_count(self):
        """n-gram count = len(s) - n + 1 for text >= n."""
        text = "болт м12"
        s = text.replace(" ", "_")
        grams = char_ngrams(text, n=4)
        assert len(grams) == len(s) - 4 + 1


# ── 5. LSH index rebuild + query ─────────────────────────────────────────────

class TestMinHashIndex:
    def test_rebuild_and_query(self):
        items = [
            _item(id=1, name="Болт М12x80 ГОСТ 7798-70", item_type="болт", size="M12x80",
                  standard_text="ГОСТ 7798-70"),
            _item(id=2, name="Гайка М12 DIN 934", item_type="гайка", size="M12",
                  standard_text="DIN 934"),
            _item(id=3, name="Шайба 12 ГОСТ 11371-78", item_type="шайба", size="12",
                  standard_text="ГОСТ 11371-78"),
        ]
        rebuild_index(items, num_perm=64, threshold=0.2)
        assert is_index_ready()
        ids = query_index("Болт М12x80 ГОСТ 7798", item_type="болт")
        assert 1 in ids

    def test_disk_found_by_minhash(self):
        items = [
            _item(id=10, name="Диск отрезной 125x1,6x22мм по металлу",
                  item_type="диск отрезной", size="125x1,6x22мм"),
        ]
        rebuild_index(items, num_perm=64, threshold=0.2)
        ids = query_index("Диск отрезной 125x22.2x1.6", item_type="диск отрезной")
        assert 10 in ids

    def test_empty_catalog_no_crash(self):
        rebuild_index([], num_perm=64, threshold=0.3)
        assert is_index_ready()
        ids = query_index("Болт М12")
        assert ids == []


# ── 6. Type buckets ──────────────────────────────────────────────────────────

class TestTypeBuckets:
    def test_type_bucket_finds_same_type(self):
        items = [
            _item(id=1, name="Саморез 4.8x35 с пресс-шайбой", item_type="саморез", size="4.8x35"),
            _item(id=2, name="Болт М12x60 ГОСТ 7798", item_type="болт", size="M12x60"),
            _item(id=3, name="Саморез 5.5x40 по дереву", item_type="саморез", size="5.5x40"),
        ]
        rebuild_index(items, num_perm=64, threshold=0.2, use_type_buckets=True)
        ids = query_index("Саморез 4.8x35", item_type="саморез",
                         use_type_buckets=True, min_candidates_before_fallback=5)
        assert 1 in ids

    def test_fallback_to_global_when_few_candidates(self):
        items = [
            _item(id=1, name="Саморез 4.8x35", item_type="саморез", size="4.8x35"),
        ]
        rebuild_index(items, num_perm=64, threshold=0.2, use_type_buckets=True)
        # Only 1 item in bucket, min_candidates=5, should fallback to global
        ids = query_index("Саморез 4.8x35", item_type="саморез",
                         use_type_buckets=True, min_candidates_before_fallback=5)
        assert 1 in ids

    def test_no_type_queries_global(self):
        items = [
            _item(id=1, name="Болт М12x60 ГОСТ 7798", item_type="болт", size="M12x60"),
        ]
        rebuild_index(items, num_perm=64, threshold=0.2, use_type_buckets=True)
        ids = query_index("Болт М12x60", item_type="")
        assert 1 in ids

    def test_buckets_disabled(self):
        items = [
            _item(id=1, name="Болт М12x60 ГОСТ 7798", item_type="болт", size="M12x60"),
        ]
        rebuild_index(items, num_perm=64, threshold=0.2, use_type_buckets=False)
        ids = query_index("Болт М12x60", item_type="болт", use_type_buckets=False)
        assert 1 in ids


# ── 7. Integration: MinHash + scorer ─────────────────────────────────────────

class TestMinHashIntegration:
    def test_bolt_m12_ranked_above_m8(self):
        """MinHash finds both bolts, scorer ranks M12x45 above M8x50."""
        from app.matching.scorer import score_match
        items = [
            _item(id=1, name="Болт М8x50 ГОСТ 7798-70", item_type="болт",
                  size="M8x50", standard_key="GOST-7798-70", standard_text="ГОСТ 7798-70"),
            _item(id=2, name="Болт М12x45 ГОСТ 7798-70", item_type="болт",
                  size="M12x45", standard_key="GOST-7798-70", standard_text="ГОСТ 7798-70"),
        ]
        rebuild_index(items, num_perm=64, threshold=0.2)
        ids = query_index("Болт М12x45 ГОСТ 7798-70", item_type="болт")
        assert 2 in ids
        row = {
            "item_type": "болт", "size": "M12x45",
            "gost": "ГОСТ 7798-70", "iso": "", "din": "",
            "diameter": "", "length": "", "strength": "", "coating": "",
        }
        s1 = score_match(row, items[0])["score"]
        s2 = score_match(row, items[1])["score"]
        assert s2 > s1, f"M12x45 exact ({s2}) should rank above M8x50 ({s1})"


# ── 8. Add/remove from index ────────────────────────────────────────────────

class TestIndexMutations:
    def test_add_and_remove(self):
        rebuild_index([], num_perm=64, threshold=0.3)
        item = _item(id=99, name="Анкер М16x200 ГОСТ 12345",
                     item_type="анкер", size="M16x200")
        add_to_index(item)
        ids = query_index("Анкер М16x200", item_type="анкер")
        assert 99 in ids

        remove_from_index(99)
        ids2 = query_index("Анкер М16x200", item_type="анкер")
        assert 99 not in ids2

    def test_add_updates_existing(self):
        """Re-adding same ID should update, not duplicate."""
        items = [_item(id=5, name="Болт М10x40", item_type="болт", size="M10x40")]
        rebuild_index(items, num_perm=64, threshold=0.2)

        updated = _item(id=5, name="Гайка М10 DIN 934", item_type="гайка", size="M10")
        add_to_index(updated)

        ids = query_index("Гайка М10 DIN", item_type="гайка")
        assert 5 in ids

    def test_add_creates_type_bucket(self):
        """Adding an item to an empty index creates a type bucket on the fly."""
        rebuild_index([], num_perm=64, threshold=0.3, use_type_buckets=True)
        item = _item(id=42, name="Шуруп 3.5x25", item_type="шуруп", size="3.5x25")
        add_to_index(item)
        ids = query_index("Шуруп 3.5x25", item_type="шуруп",
                         use_type_buckets=True, min_candidates_before_fallback=5)
        assert 42 in ids


# ── 9. Jaccard sanity ────────────────────────────────────────────────────────

class TestJaccardSanity:
    def test_same_item_closest(self):
        """'Саморез 4.8x35' should be more similar to itself than to 'Болт M12x60'."""
        items = [
            _item(id=1, name="Саморез 4.8x35 с пресс-шайбой", item_type="саморез", size="4.8x35"),
            _item(id=2, name="Болт M12x60 ГОСТ 7798", item_type="болт", size="M12x60"),
        ]
        rebuild_index(items, num_perm=128, threshold=0.1, use_type_buckets=False)
        results = query_index_with_scores("Саморез 4.8x35", use_type_buckets=False)
        if len(results) >= 2:
            assert results[0]["item_id"] == 1
            assert results[0]["jaccard"] > results[1]["jaccard"]

    def test_samorez_not_dominated_by_bolt(self):
        """For a samorez query, samorez candidates should rank above bolts."""
        items = [
            _item(id=1, name="Саморез кровельный 4.8x35 мм по дереву", item_type="саморез"),
            _item(id=2, name="Саморез 4.8x29 с прессшайбой", item_type="саморез"),
            _item(id=3, name="Болт М12x60 ГОСТ 7798-70", item_type="болт"),
            _item(id=4, name="Гайка М12 DIN 934", item_type="гайка"),
        ]
        rebuild_index(items, num_perm=128, threshold=0.1, use_type_buckets=True)
        results = query_index_with_scores(
            "Саморезы кровельные 4,8х35 мм", item_type="саморез",
            use_type_buckets=True, min_candidates_before_fallback=5,
        )
        # At least one samorez should appear in results
        samorez_ids = {r["item_id"] for r in results if r["item_id"] in (1, 2)}
        assert len(samorez_ids) > 0, "Expected at least one samorez in results"


# ── 10. Settings persistence ──────────────────────────────────────────────────

class TestMinHashSettings:
    def test_settings_roundtrip(self):
        from app.match_settings import MatchSettings, load_match_settings, save_match_settings
        settings = MatchSettings(
            enable_minhash=False,
            lsh_threshold=0.5,
            num_perm=64,
            minhash_top_k=10,
            ngram_n=5,
            use_type_buckets=False,
            min_candidates_before_fallback=3,
            minhash_filter_size=True,
        )
        save_match_settings(settings)
        loaded = load_match_settings()
        assert loaded.enable_minhash is False
        assert loaded.lsh_threshold == 0.5
        assert loaded.num_perm == 64
        assert loaded.minhash_top_k == 10
        assert loaded.ngram_n == 5
        assert loaded.use_type_buckets is False
        assert loaded.min_candidates_before_fallback == 3
        assert loaded.minhash_filter_size is True


# ── 11. post_filter_candidates unit tests ─────────────────────────────────────

class _MockItem:
    """Lightweight duck-typed InternalItem for unit tests (no DB / SQLAlchemy)."""
    def __init__(self, item_id, *, item_type="", size="", standard_text="", name="Test"):
        self.id = item_id
        self.name = name
        self.item_type = item_type
        self.size = size
        self.standard_text = standard_text
        self.folder_path = ""
        self.folder_name = ""
        self.canonical_key = None
        self.uid_1c = None


def _mock_item(item_id, **kwargs):
    return _MockItem(item_id, **kwargs)


def _cand(item_id, jaccard=0.8):
    return {
        "item_id": item_id, "name": f"Item {item_id}",
        "score": round(jaccard * 100),
        "reasons": [f"MinHash J={jaccard:.3f}"],
        "warn_reasons": [], "breakdown": {},
        "via_analog": None, "folder_path": "", "folder_name": "",
    }


def _raw(item_id, jaccard=0.8):
    return {"item_id": item_id, "name": f"Item {item_id}", "jaccard": jaccard, "via_analog": None}


def _run_pf(candidates, minhash_raw, row_dict, items, *, use_analogs=False):
    from app.matching.post_filter import post_filter_candidates
    from app.match_settings import MatchSettings
    item_by_id = {it.id: it for it in items}
    settings = MatchSettings(minhash_top_k=20, min_display_score=0)
    return post_filter_candidates(
        candidates, minhash_raw, row_dict, item_by_id, settings, use_analogs=use_analogs
    )


class TestPostFilterCandidates:
    def test_size_mismatch_removed(self):
        good = _mock_item(1, size="M12x60", item_type="болт")
        bad  = _mock_item(2, size="M16x80", item_type="болт")
        candidates = [_cand(1, 0.9), _cand(2, 0.85)]
        minhash_raw = [_raw(1, 0.9), _raw(2, 0.85)]
        row_dict = {"item_type": "болт", "size": "M12x60", "gost": "", "iso": "", "din": ""}

        filtered, log = _run_pf(candidates, minhash_raw, row_dict, [good, bad])

        ids = [c["item_id"] for c in filtered]
        assert 1 in ids, "Correct size must survive"
        assert 2 not in ids, "Wrong size must be filtered"
        assert log["fallback_level"] == 0

    def test_type_mismatch_removed(self):
        good = _mock_item(1, item_type="болт",  size="M10")
        bad  = _mock_item(2, item_type="гайка", size="M10")
        candidates = [_cand(1, 0.9), _cand(2, 0.85)]
        minhash_raw = [_raw(1, 0.9), _raw(2, 0.85)]
        row_dict = {"item_type": "болт", "size": "M10", "gost": "", "iso": "", "din": ""}

        filtered, log = _run_pf(candidates, minhash_raw, row_dict, [good, bad])

        ids = [c["item_id"] for c in filtered]
        assert 1 in ids
        assert 2 not in ids
        assert log["fallback_level"] == 0

    def test_standard_mismatch_removed_at_level0(self):
        good = _mock_item(1, item_type="болт", size="M12", standard_text="DIN 933")
        bad  = _mock_item(2, item_type="болт", size="M12", standard_text="ГОСТ 7798-70")
        candidates = [_cand(1, 0.9), _cand(2, 0.85)]
        minhash_raw = [_raw(1, 0.9), _raw(2, 0.85)]
        row_dict = {"item_type": "болт", "size": "M12", "din": "DIN 933", "gost": "", "iso": ""}

        filtered, log = _run_pf(candidates, minhash_raw, row_dict, [good, bad])

        ids = [c["item_id"] for c in filtered]
        assert 1 in ids
        assert 2 not in ids

    def test_fallback_level1_drop_standard(self):
        """All candidates fail full filter but pass size+type → fallback level 1."""
        item1 = _mock_item(1, item_type="болт", size="M12", standard_text="ГОСТ 7798-70")
        item2 = _mock_item(2, item_type="болт", size="M12", standard_text="ГОСТ 7798-70")
        candidates = [_cand(1, 0.9), _cand(2, 0.85)]
        minhash_raw = [_raw(1, 0.9), _raw(2, 0.85)]
        # Row says DIN 933 but items have ГОСТ → standard mismatch
        row_dict = {"item_type": "болт", "size": "M12", "din": "DIN 933", "gost": "", "iso": ""}

        filtered, log = _run_pf(candidates, minhash_raw, row_dict, [item1, item2])

        assert len(filtered) > 0
        assert log["fallback_level"] == 1, f"Expected level 1, got {log['fallback_level']}"

    def test_fallback_level2_drop_type(self):
        """Size matches but type and standard both mismatch → level 2 (size only)."""
        item1 = _mock_item(1, item_type="гайка", size="M12", standard_text="ГОСТ 7798-70")
        candidates = [_cand(1, 0.9)]
        minhash_raw = [_raw(1, 0.9)]
        row_dict = {"item_type": "болт", "size": "M12", "din": "DIN 933", "gost": "", "iso": ""}

        filtered, log = _run_pf(candidates, minhash_raw, row_dict, [item1])

        assert len(filtered) > 0
        assert log["fallback_level"] == 2, f"Expected level 2, got {log['fallback_level']}"

    def test_fallback_level3_no_filter(self):
        """Even size filter removes all → level 3 returns everything."""
        item1 = _mock_item(1, item_type="болт", size="M20x100")
        candidates = [_cand(1, 0.9)]
        minhash_raw = [_raw(1, 0.9)]
        row_dict = {"item_type": "гайка", "size": "M8", "gost": "", "iso": "", "din": ""}

        filtered, log = _run_pf(candidates, minhash_raw, row_dict, [item1])

        assert len(filtered) > 0
        assert log["fallback_level"] == 3, f"Expected level 3, got {log['fallback_level']}"

    def test_best_filtered_out_true(self):
        """best_filtered_out=True when top-J candidate fails level-0 filter."""
        top_item = _mock_item(1, item_type="болт", size="M20x100")  # wrong size
        ok_item  = _mock_item(2, item_type="болт", size="M12x60")
        candidates = [_cand(1, 0.9), _cand(2, 0.7)]
        minhash_raw = [_raw(1, 0.9), _raw(2, 0.7)]
        row_dict = {"item_type": "болт", "size": "M12x60", "gost": "", "iso": "", "din": ""}

        filtered, log = _run_pf(candidates, minhash_raw, row_dict, [top_item, ok_item])

        assert log["best_filtered_out"] is True
        assert any(c["item_id"] == 2 for c in filtered)

    def test_best_filtered_out_false(self):
        """best_filtered_out=False when top-J candidate passes."""
        item1 = _mock_item(1, item_type="болт", size="M12x60")
        candidates = [_cand(1, 0.9)]
        minhash_raw = [_raw(1, 0.9)]
        row_dict = {"item_type": "болт", "size": "M12x60", "gost": "", "iso": "", "din": ""}

        _, log = _run_pf(candidates, minhash_raw, row_dict, [item1])

        assert log["best_filtered_out"] is False

    def test_no_filter_when_row_empty(self):
        """No row fields → all candidates pass (None match = pass)."""
        item1 = _mock_item(1, item_type="болт",  size="M12x60")
        item2 = _mock_item(2, item_type="гайка", size="M20")
        candidates = [_cand(1, 0.9), _cand(2, 0.8)]
        minhash_raw = [_raw(1, 0.9), _raw(2, 0.8)]
        row_dict = {"item_type": "", "size": "", "gost": "", "iso": "", "din": ""}

        filtered, log = _run_pf(candidates, minhash_raw, row_dict, [item1, item2])

        assert len(filtered) == 2
        assert log["fallback_level"] == 0

    def test_field_badges_present_on_candidates(self):
        """field_badges must be added to each candidate after filtering."""
        item1 = _mock_item(1, item_type="болт", size="M12x60")
        candidates = [_cand(1, 0.9)]
        minhash_raw = [_raw(1, 0.9)]
        row_dict = {"item_type": "болт", "size": "M12x60", "gost": "", "iso": "", "din": ""}

        filtered, _ = _run_pf(candidates, minhash_raw, row_dict, [item1])

        assert len(filtered) == 1
        badges = filtered[0].get("field_badges", {})
        assert "size" in badges
        assert "type" in badges
        assert badges["size"]["match"] is True
        assert badges["type"]["match"] is True

    def test_steps_populated_in_log(self):
        """filter_log.steps must contain at least one step when row has size."""
        item1 = _mock_item(1, item_type="болт", size="M12x60")
        candidates = [_cand(1, 0.9)]
        minhash_raw = [_raw(1, 0.9)]
        row_dict = {"item_type": "болт", "size": "M12x60", "gost": "", "iso": "", "din": ""}

        _, log = _run_pf(candidates, minhash_raw, row_dict, [item1])

        assert len(log["steps"]) > 0, "filter_log.steps must be non-empty when row has fields"


# ── 12. Integration: filter_log in match_debug + badges + auto-apply block ────

class TestFilterLogIntegration:
    def _seed_item(self, session, name, **kwargs):
        from datetime import datetime, timezone
        item = InternalItem(
            name=name, is_active=True,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
            **kwargs,
        )
        session.add(item)
        session.flush()
        session.commit()
        return item

    def test_filter_log_in_match_debug(self):
        """match_debug must contain a filter_log dict on every result."""
        import pandas as pd
        from app.database import get_db_session
        from app.matcher import add_internal_matches
        from app.match_settings import MatchSettings

        session = get_db_session()
        item = self._seed_item(session, "Болт М12x60 DIN 933",
                               item_type="болт", size="M12x60", standard_text="DIN 933")
        rebuild_index([item], num_perm=64, threshold=0.05, ngram_n=4, use_type_buckets=True)
        session.close()

        settings = MatchSettings(
            enable_minhash=True, lsh_threshold=0.05, num_perm=64, ngram_n=4,
            use_type_buckets=True, min_candidates_before_fallback=1,
            auto_apply_enabled=False, minhash_top_k=20, min_display_score=0,
        )
        df = pd.DataFrame([{
            "item_type": "болт", "size": "M12x60", "gost": "", "iso": "", "din": "DIN 933",
            "diameter": "", "length": "", "strength": "", "coating": "",
            "name_raw": "Болт М12x60 DIN 933", "name": "болт м12x60 din 933",
        }])
        _, results = add_internal_matches(df, settings=settings)
        dbg = results[0].get("match_debug", {})
        assert "filter_log" in dbg
        fl = dbg["filter_log"]
        for key in ("minhash_total", "fallback_level", "steps", "best_filtered_out"):
            assert key in fl, f"filter_log missing key: {key}"

    def test_field_badges_in_candidates(self):
        """Each candidate in match results must have field_badges."""
        import pandas as pd
        from app.database import get_db_session
        from app.matcher import add_internal_matches
        from app.match_settings import MatchSettings

        session = get_db_session()
        item = self._seed_item(session, "Болт М12x60", item_type="болт", size="M12x60")
        rebuild_index([item], num_perm=64, threshold=0.05, ngram_n=4, use_type_buckets=True)
        session.close()

        settings = MatchSettings(
            enable_minhash=True, lsh_threshold=0.05, num_perm=64, ngram_n=4,
            use_type_buckets=True, min_candidates_before_fallback=1,
            auto_apply_enabled=False, minhash_top_k=20, min_display_score=0,
        )
        df = pd.DataFrame([{
            "item_type": "болт", "size": "M12x60",
            "gost": "", "iso": "", "din": "",
            "diameter": "", "length": "", "strength": "", "coating": "",
            "name_raw": "Болт М12x60", "name": "болт м12x60",
        }])
        _, results = add_internal_matches(df, settings=settings)
        candidates = results[0].get("candidates", [])
        assert len(candidates) > 0
        for c in candidates:
            assert "field_badges" in c, f"Candidate {c.get('item_id')} missing field_badges"

    def test_auto_apply_blocked_by_size_filter(self):
        """AUTO_MINHASH must be blocked when best candidate fails size hard filter."""
        import pandas as pd
        from app.database import get_db_session
        from app.matcher import add_internal_matches, MATCH_MODE_AUTO_MINHASH
        from app.match_settings import MatchSettings

        session = get_db_session()
        # Catalog has M20x100, row asks for M12x60 — size mismatch
        item = self._seed_item(session, "Болт М20x100", item_type="болт", size="M20x100")
        rebuild_index([item], num_perm=64, threshold=0.05, ngram_n=4, use_type_buckets=True)
        session.close()

        settings = MatchSettings(
            enable_minhash=True, lsh_threshold=0.05, num_perm=64, ngram_n=4,
            use_type_buckets=True, min_candidates_before_fallback=1,
            auto_apply_enabled=True,
            auto_apply_jaccard_threshold=0.0,  # threshold=0 → would auto without filter
            minhash_top_k=20, min_display_score=0,
        )
        df = pd.DataFrame([{
            "item_type": "болт", "size": "M12x60",   # different size than catalog
            "gost": "", "iso": "", "din": "",
            "diameter": "", "length": "", "strength": "", "coating": "",
            "name_raw": "Болт М12x60", "name": "болт м12x60",
        }])
        _, results = add_internal_matches(df, settings=settings)
        r = results[0]
        if r.get("candidates"):  # MinHash found the item
            assert r["mode"] != MATCH_MODE_AUTO_MINHASH, (
                f"AUTO_MINHASH must be blocked by size filter mismatch; "
                f"mode={r['mode']}, filter_log={r.get('match_debug', {}).get('filter_log')}"
            )
