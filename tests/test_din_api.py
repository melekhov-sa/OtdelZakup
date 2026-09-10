"""analog_mode parameter of the 1C API."""
import pytest

from app.match_settings import MatchSettings


def _base():
    return MatchSettings()


def test_no_params_keeps_settings():
    from app.api import _resolve_analog_mode
    s = _base()
    assert _resolve_analog_mode(s, None, None) is s


def test_din_mode():
    from app.api import _resolve_analog_mode
    got = _resolve_analog_mode(_base(), "din", None)
    assert got.din_only is True
    assert got.analogs_only is False
    assert got.use_standard_analogs_in_main_match is False


def test_only_mode():
    from app.api import _resolve_analog_mode
    got = _resolve_analog_mode(_base(), "only", None)
    assert got.analogs_only is True
    assert got.din_only is False


def test_legacy_use_analogs_true():
    from app.api import _resolve_analog_mode
    got = _resolve_analog_mode(_base(), None, True)
    assert got.use_standard_analogs_in_main_match is True
    assert got.din_only is False


def test_legacy_use_analogs_false():
    from app.api import _resolve_analog_mode
    got = _resolve_analog_mode(_base(), None, False)
    assert got.use_standard_analogs_in_main_match is False
    assert got.din_only is False


def test_analog_mode_wins_over_legacy():
    from app.api import _resolve_analog_mode
    got = _resolve_analog_mode(_base(), "din", True)
    assert got.din_only is True
    assert got.use_standard_analogs_in_main_match is False


def test_unknown_value_is_ignored():
    from app.api import _resolve_analog_mode
    s = _base()
    assert _resolve_analog_mode(s, "DIN-931", None) is s


@pytest.mark.parametrize("raw", ["DIN", " din ", "Din"])
def test_value_is_case_insensitive(raw):
    from app.api import _resolve_analog_mode
    assert _resolve_analog_mode(_base(), raw, None).din_only is True
