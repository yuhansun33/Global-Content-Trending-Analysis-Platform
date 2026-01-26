"""Unit tests for the Kafka producer."""

import sys
from pathlib import Path

# Add workspace to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "workspace"))


def test_base_country_weights():
    """Test that base country weights sum correctly and match expected distribution."""
    from producer import BASE_COUNTRY_WEIGHTS, COUNTRIES

    assert len(COUNTRIES) == len(BASE_COUNTRY_WEIGHTS)
    assert len(COUNTRIES) == 10

    # US should have highest base weight
    us_index = COUNTRIES.index("US")
    assert BASE_COUNTRY_WEIGHTS[us_index] == max(BASE_COUNTRY_WEIGHTS)

    # Total base weight should be 196 million
    assert sum(BASE_COUNTRY_WEIGHTS) == 196


def test_country_list():
    """Test that all expected countries are present."""
    from producer import COUNTRIES

    expected = {"US", "UK", "DE", "BR", "MX", "FR", "IN", "CA", "JP", "KR"}
    assert set(COUNTRIES) == expected


def test_country_timezones():
    """Test that all countries have timezone mappings."""
    from producer import COUNTRIES, COUNTRY_TIMEZONES

    for country in COUNTRIES:
        assert country in COUNTRY_TIMEZONES
        # Verify timezone is valid
        import pytz

        assert COUNTRY_TIMEZONES[country] in pytz.all_timezones


def test_dynamic_weights():
    """Test that dynamic weights function returns correct structure."""
    from producer import COUNTRIES, get_dynamic_country_weights

    weights = get_dynamic_country_weights()
    assert len(weights) == len(COUNTRIES)
    # All weights should be positive
    assert all(w > 0 for w in weights)


def test_generate_viewing_event_structure():
    """Test that generated events have correct structure."""
    from producer import generate_viewing_event

    # Mock movies data
    mock_movies = [{"movie_id": "m001", "genre": "Action,Drama"}]

    event = generate_viewing_event(mock_movies)

    # Check all required fields exist
    assert "user_id" in event
    assert "movie_id" in event
    assert "country" in event
    assert "genre" in event
    assert "watch_hours" in event
    assert "timestamp" in event

    # Check field types
    assert event["user_id"].startswith("user_")
    assert event["movie_id"] == "m001"
    assert 0.1 <= event["watch_hours"] <= 3.0
    assert event["country"] in {
        "US",
        "UK",
        "DE",
        "BR",
        "MX",
        "FR",
        "IN",
        "CA",
        "JP",
        "KR",
    }


def test_get_peak_countries():
    """Test that get_peak_countries returns valid structure."""
    from producer import get_peak_countries

    peak_countries = get_peak_countries()

    # Should return a list
    assert isinstance(peak_countries, list)

    # Each item should be a tuple of (country_code, time_string)
    for item in peak_countries:
        assert isinstance(item, tuple)
        assert len(item) == 2
        country, time_str = item
        assert country in {"US", "UK", "DE", "BR", "MX", "FR", "IN", "CA", "JP", "KR"}
        assert ":" in time_str  # Time format like "20:30"


def test_batch_config():
    """Test that batch configuration is valid."""
    from producer import BATCH_MAX, BATCH_MIN, EVENT_INTERVAL

    assert BATCH_MIN > 0
    assert BATCH_MAX >= BATCH_MIN
    assert EVENT_INTERVAL > 0
