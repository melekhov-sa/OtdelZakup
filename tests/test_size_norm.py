"""Tests for size normalization and strict size filtering.

Covers:
1. normalize_size: canonical uppercase form, dash/space handling, Cyrillic
2. Strict post-filter: size_no_match=True when size known but no candidate matches
3. Filter passes all candidates when size is unknown (query.size_norm = None)
4. MinHash tokens include SIZE:, TYPE:, STD: special tokens
"""
import pytest

from app.matching.normalizer import normalize_size
from app.models import InternalItem


# ── Helpers ────────────────────────────────────────────────────────────────────

def _item(**kwargs):
    defaults = dict(is_active=True, name="Test")
    defaults.update(kwargs)
    return InternalItem(**defaults)


def _candidate(item_id: int, jaccard: float = 0.5) -> dict:
    return {
        "item_id": item_id,
        "name": f"Item {item_id}",
        "score": round(jaccard * 100),
        "reasons": [],
        "warn_reasons": [],
        "breakdown": {},
        "via_analog": None,
        "folder_path": "",
        "folder_name": "",
    }


def _minhash_raw(item_id: int, jaccard: float = 0.5) -> dict:
    return {"item_id": item_id, "jaccard": jaccard, "via_analog": None}


# ── 1. normalize_size ──────────────────────────────────────────────────────────

class TestNormalizeSize:

    def test_cyrillic_m(self):
        assert normalize_size("М24") == "M24"

    def test_lowercase_with_space(self):
        assert normalize_size("m 24") == "M24"

    def test_dash_between_prefix_and_digit(self):
        assert normalize_size("M-24") == "M24"

    def test_lowercase_x_separator(self):
        assert normalize_size("M24x50") == "M24X50"

    def test_unicode_times_separator(self):
        assert normalize_size("M24×50") == "M24X50"

    def test_cyrillic_with_x(self):
        """Full Cyrillic: М and Х both transliterated."""
        assert normalize_size("М12Х60") == "M12X60"

    def test_space_around_x(self):
        assert normalize_size("M12 x 80") == "M12X80"

    def test_decimal_comma(self):
        assert normalize_size("4,2x70") == "4.2X70"

    def test_trailing_mm(self):
        assert normalize_size("125x1,6x22мм") == "125X1.6X22"

    def test_empty_string(self):
        assert normalize_size("") == ""

    def test_plain_number(self):
        """Non-M-prefix size is still uppercased."""
        assert normalize_size("8x8x50") == "8X8X50"

    def test_already_normalized(self):
        assert normalize_size("M24X50") == "M24X50"

    def test_multiple_dashes(self):
        """Only the first dash between M and digit is removed."""
        assert normalize_size("M-12x50") == "M12X50"

    def test_cyrillic_lowercase_m(self):
        assert normalize_size("м12") == "M12"


# ── 2. Strict size filter ──────────────────────────────────────────────────────

class TestStrictSizeFilter:
    """post_filter_candidates must return empty when size is known but no match."""

    def _run_filter(self, row_size: str, item_sizes: list[str], settings=None):
        from app.matching.post_filter import post_filter_candidates

        class FakeSettings:
            use_standard_analogs_in_main_match = False

        settings = settings or FakeSettings()

        items = {}
        candidates = []
        minhash_raw = []
        for i, sz in enumerate(item_sizes, start=1):
            it = _item(id=i, name=f"Item {i}", size=sz)
            items[i] = it
            c = _candidate(i, jaccard=0.6)
            candidates.append(c)
            minhash_raw.append(_minhash_raw(i, jaccard=0.6))

        row_dict = {"size": row_size, "item_type": "", "gost": "", "iso": "", "din": ""}
        return post_filter_candidates(candidates, minhash_raw, row_dict, items, settings)

    def test_strict_filter_removes_wrong_sizes(self):
        """query size=M24, candidates M6/M16/M24 → only M24 survives."""
        filtered, log = self._run_filter("M24", ["M6", "M16", "M24"])
        assert len(filtered) == 1
        assert filtered[0]["item_id"] == 3  # M24 is item 3

    def test_strict_filter_returns_empty_when_no_match(self):
        """query size=M24, candidates M6/M16 → empty list + size_no_match=True."""
        filtered, log = self._run_filter("M24", ["M6", "M16"])
        assert filtered == []
        assert log["size_no_match"] is True

    def test_strict_filter_no_size_passes_all(self):
        """When row size is empty, filter not applied → all candidates pass."""
        filtered, log = self._run_filter("", ["M6", "M16", "M24"])
        assert len(filtered) == 3
        assert log["size_no_match"] is False

    def test_fallback_not_past_size(self):
        """With size known, fallback level must not reach 3 (no-filter) for empty result."""
        filtered, log = self._run_filter("M24", ["M6"])
        # Returns empty; fallback_level is 3 but size_no_match=True (strict)
        assert filtered == []
        assert log["size_no_match"] is True

    def test_cyrillic_size_normalized_for_comparison(self):
        """Row 'М24' and catalog item size 'M24' must match after normalization."""
        filtered, log = self._run_filter("М24", ["M6", "M24", "M16"])
        assert len(filtered) == 1
        assert filtered[0]["item_id"] == 2  # M24 is item 2


# ── 3. MinHash special tokens ──────────────────────────────────────────────────

class TestMinhashSpecialTokens:
    """_item_ngrams must include SIZE:, TYPE:, STD: tokens."""

    def test_size_token_present(self):
        from app.matching.minhash_index import _item_ngrams
        item = _item(id=1, name="Шайба M24 DIN 125", size="M24", item_type="шайба", standard_key="DIN-125")
        tokens = _item_ngrams(item, ngram_n=4)
        assert "SIZE:M24" in tokens

    def test_type_token_present(self):
        from app.matching.minhash_index import _item_ngrams
        item = _item(id=1, name="Шайба M24", size="M24", item_type="шайба")
        tokens = _item_ngrams(item, ngram_n=4)
        assert "TYPE:шайба" in tokens

    def test_std_token_present(self):
        from app.matching.minhash_index import _item_ngrams
        item = _item(id=1, name="Болт M12 DIN 933", size="M12", item_type="болт", standard_key="DIN-933")
        tokens = _item_ngrams(item, ngram_n=4)
        assert "STD:DIN-933" in tokens

    def test_no_size_no_size_token(self):
        from app.matching.minhash_index import _item_ngrams
        item = _item(id=1, name="Прокладка резиновая", size=None)
        tokens = _item_ngrams(item, ngram_n=4)
        assert not any(t.startswith("SIZE:") for t in tokens)

    def test_cyrillic_size_token_normalized(self):
        """Item with Cyrillic size 'М24' must produce SIZE:M24 (Latin uppercase)."""
        from app.matching.minhash_index import _item_ngrams
        item = _item(id=1, name="Шайба М24", size="М24", item_type="шайба")
        tokens = _item_ngrams(item, ngram_n=4)
        assert "SIZE:M24" in tokens
        assert "SIZE:М24" not in tokens  # Cyrillic form must NOT appear
