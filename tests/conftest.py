"""Global test fixtures.

Resets module-level caches between tests so data loaded from one test's
tmp_path DB does not leak into the next test with a different DB.
"""

import pytest


def _reset_all_caches() -> None:
    """Drop every process-wide cache the app keeps.

    Module-level caching is a feature in production — it avoids reloading 220k
    catalog rows and every rule table on each request.  In the test suite it is
    a hazard: each test gets its own tmp_path database, so a cache filled from
    the previous test's DB describes rules and items that no longer exist.
    """
    from app import catalog_cache, category_validator, inference_engine
    from app import match_settings, matcher, product_type_matcher, readiness
    from app.matching import standard_analogs
    from app.parsing import tail_extractor
    from app.services import (
        coating_detector,
        normalization_service,
        quote_order_matcher,
        size_detector,
        strength_detector,
    )

    catalog_cache.invalidate()
    category_validator.invalidate_category_validator_cache()
    inference_engine.invalidate_inference_cache()
    match_settings.invalidate_settings_cache()
    matcher.invalidate_match_memory_cache()
    matcher.invalidate_master_guid_cache()
    product_type_matcher.invalidate_product_types_cache()
    readiness.invalidate_readiness_caches()
    standard_analogs.invalidate_standard_analogs_cache()
    tail_extractor.invalidate_tail_phrases_cache()
    coating_detector.invalidate_coating_cache()
    normalization_service.invalidate_normalization_cache()
    size_detector.invalidate_size_cache()
    strength_detector.invalidate_strength_cache()

    # These two hold plain module-level state with no invalidator of their own
    matcher._type_size_idx_cache = None
    quote_order_matcher._index_cache.clear()


@pytest.fixture(autouse=True)
def _reset_module_caches():
    """Clear cached state before AND after every test."""
    _reset_all_caches()
    yield
    _reset_all_caches()
