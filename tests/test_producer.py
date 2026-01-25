"""Unit tests for the Kafka producer."""
import sys
from pathlib import Path

# Add workspace to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "workspace"))


def test_country_weights():
    """Test that country weights sum correctly and match expected distribution."""
    from producer import COUNTRIES, COUNTRY_WEIGHTS

    assert len(COUNTRIES) == len(COUNTRY_WEIGHTS)
    assert len(COUNTRIES) == 10

    # US should have highest weight
    us_index = COUNTRIES.index("US")
    assert COUNTRY_WEIGHTS[us_index] == max(COUNTRY_WEIGHTS)

    # Total weight should be 196 million
    assert sum(COUNTRY_WEIGHTS) == 196


def test_country_list():
    """Test that all expected countries are present."""
    from producer import COUNTRIES

    expected = {"US", "UK", "DE", "BR", "MX", "FR", "IN", "CA", "JP", "KR"}
    assert set(COUNTRIES) == expected


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
    assert event["country"] in {"US", "UK", "DE", "BR", "MX", "FR", "IN", "CA", "JP", "KR"}
