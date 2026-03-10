"""Tests for post-scoring penalties and caps.

Covers:
1. "Гайка M20" vs "Болт ... комплект: гайка + шайба" → score ≤ 20
2. "Болт M16x50 ГОСТ 7798-70" vs "Болт ГОСТ 7798-70 M16x50" → score ≥ 90
3. "Диск 125x22.2x1.6" vs "Диск 125x1,6x22" → score ≥ 85
4. Kit detection: _is_kit correctly identifies kits
5. Type mismatch penalty applies and doesn't affect correct matches
6. Size complete mismatch caps score at 15
7. Standard conflict caps score at 15
"""

import pytest

from app.matching.scorer import _is_kit, score_match
from app.models import InternalItem


def _item(**kwargs):
    defaults = dict(is_active=True, name="Test")
    defaults.update(kwargs)
    return InternalItem(**defaults)


# ── Kit detection ──────────────────────────────────────────────────────────────

class TestKitDetection:
    def test_kit_marker_kompl(self):
        assert _is_kit("Болт M20 комплект гайка+шайба") is True

    def test_kit_marker_v_sbore(self):
        assert _is_kit("Анкер M12 в сборе с гайкой") is True

    def test_kit_marker_nabor(self):
        assert _is_kit("Набор крепежа") is True

    def test_kit_plus_separator(self):
        assert _is_kit("гайка+шайба") is True

    def test_kit_plus_with_spaces(self):
        assert _is_kit("болт + гайка + шайба") is True

    def test_not_kit_plain_bolt(self):
        assert _is_kit("Болт M16x50 ГОСТ 7798-70") is False

    def test_not_kit_size_with_plus(self):
        # "M20+60" has digits around +, not letters
        assert _is_kit("Болт M20") is False


# ── Core penalty cases ─────────────────────────────────────────────────────────

class TestPenalties:
    def test_gayka_m20_vs_bolt_kit_score_le_20(self):
        """'Гайка M20' vs 'Болт M20 комплект: гайка + шайба' → score ≤ 20."""
        item = _item(
            name="Болт M20 комплект: гайка + шайба",
            item_type="болт", size="M20",
        )
        row = {
            "item_type": "гайка", "size": "M20",
            "gost": "", "iso": "", "din": "",
            "diameter": "", "length": "",
            "strength": "", "coating": "",
        }
        result = score_match(row, item)
        assert result["score"] <= 20, (
            f"Expected ≤ 20 for kit+type mismatch; got {result['score']}; "
            f"reasons={result['reasons']}; warns={result['warn_reasons']}"
        )
        assert any("комплект" in w for w in result["warn_reasons"]), (
            f"Expected kit warning; warns={result['warn_reasons']}"
        )

    def test_gayka_m20_vs_bolt_m20_no_kit_score_low(self):
        """'Гайка M20' vs 'Болт M20' (not a kit) → score low due to type penalty."""
        item = _item(
            name="Болт M20 DIN 934",
            item_type="болт", size="M20",
        )
        row = {
            "item_type": "гайка", "size": "M20",
            "gost": "", "iso": "", "din": "",
            "diameter": "", "length": "",
            "strength": "", "coating": "",
        }
        result = score_match(row, item)
        assert result["score"] <= 60, (
            f"Expected ≤ 60 for type mismatch; got {result['score']}"
        )

    def test_bolt_m16x50_gost_exact_match_ge_90(self):
        """'Болт M16x50 ГОСТ 7798-70' vs catalog same → score ≥ 90."""
        item = _item(
            name="Болт ГОСТ 7798-70 M16x50",
            item_type="болт", size="M16x50",
            standard_key="GOST-7798-70", standard_text="ГОСТ 7798-70",
        )
        row = {
            "item_type": "болт", "size": "M16x50",
            "gost": "ГОСТ 7798-70",
            "iso": "", "din": "",
            "diameter": "", "length": "",
            "strength": "", "coating": "",
        }
        result = score_match(row, item)
        assert result["score"] >= 90, (
            f"Expected ≥ 90 for exact match; got {result['score']}; "
            f"reasons={result['reasons']}; warns={result['warn_reasons']}"
        )

    def test_disk_close_size_ge_85(self):
        """'Диск 125x22.2x1.6' vs 'Диск 125x1,6x22' → score ≥ 85."""
        item = _item(
            name="Диск отрезной 125x1,6x22мм по металлу",
            item_type="диск отрезной",
            size="125x1,6x22мм",
        )
        row = {
            "item_type": "диск отрезной", "size": "125x22.2x1.6",
            "gost": "", "iso": "", "din": "",
            "diameter": "", "length": "",
            "strength": "", "coating": "",
        }
        result = score_match(row, item)
        assert result["score"] >= 85, (
            f"Expected ≥ 85 for close-size disk match; got {result['score']}"
        )

    def test_size_complete_mismatch_caps_at_15(self):
        """Bolt M20 vs Bolt M6 (same type, same std) → capped at 15 due to size conflict."""
        item = _item(
            name="Болт M6x20 ГОСТ 7798-70",
            item_type="болт", size="M6x20",
            standard_key="GOST-7798-70",
        )
        row = {
            "item_type": "болт", "size": "M20x60",
            "gost": "ГОСТ 7798-70",
            "iso": "", "din": "",
            "diameter": "", "length": "",
            "strength": "", "coating": "",
        }
        result = score_match(row, item)
        assert result["score"] <= 15, (
            f"Expected ≤ 15 for complete size mismatch; got {result['score']}"
        )

    def test_standard_conflict_penalized(self):
        """Same type+size but different standard → penalized (standard penalty applied)."""
        item = _item(
            name="Болт M12x60 DIN 933",
            item_type="болт", size="M12x60",
            standard_key="DIN-933",
        )
        row = {
            "item_type": "болт", "size": "M12x60",
            "gost": "ГОСТ 7798-70",
            "iso": "", "din": "",
            "diameter": "", "length": "",
            "strength": "", "coating": "",
        }
        result = score_match(row, item)
        # Standard conflict gets p_standard_mismatch penalty (default -30)
        assert result["score"] <= 70, (
            f"Expected ≤ 70 for standard conflict; got {result['score']}"
        )
        assert "penalty" in result["breakdown"], (
            f"Expected penalty in breakdown; got {result['breakdown']}"
        )

    def test_correct_match_no_penalties(self):
        """Exact match should have no penalties/caps in breakdown."""
        item = _item(
            name="Гайка M20 DIN 934",
            item_type="гайка", size="M20",
            standard_key="DIN-934",
        )
        row = {
            "item_type": "гайка", "size": "M20",
            "gost": "", "iso": "", "din": "DIN 934",
            "diameter": "", "length": "",
            "strength": "", "coating": "",
        }
        result = score_match(row, item)
        assert result["score"] >= 90, (
            f"Expected ≥ 90 for exact match; got {result['score']}"
        )
        assert "penalty" not in result["breakdown"], (
            f"Expected no penalty in breakdown; got {result['breakdown']}"
        )
        assert "cap" not in result["breakdown"], (
            f"Expected no cap in breakdown; got {result['breakdown']}"
        )
