from datetime import datetime, timedelta, timezone

from crawler.domain.retry_policy import compute_next_retry_at


NOW = datetime(2026, 8, 20, 12, 0, tzinfo=timezone.utc)


def test_compute_next_retry_at_honors_retry_after() -> None:
    assert compute_next_retry_at(
        now=NOW,
        attempt_number=3,
        base_backoff_seconds=10,
        max_backoff_seconds=60,
        retry_after_seconds=25,
    ) == NOW + timedelta(seconds=25)


def test_compute_next_retry_at_uses_exponential_backoff() -> None:
    assert compute_next_retry_at(
        now=NOW,
        attempt_number=3,
        base_backoff_seconds=10,
        max_backoff_seconds=60,
        retry_after_seconds=None,
    ) == NOW + timedelta(seconds=40)


def test_compute_next_retry_at_caps_exponential_backoff() -> None:
    assert compute_next_retry_at(
        now=NOW,
        attempt_number=5,
        base_backoff_seconds=10,
        max_backoff_seconds=60,
        retry_after_seconds=None,
    ) == NOW + timedelta(seconds=60)
