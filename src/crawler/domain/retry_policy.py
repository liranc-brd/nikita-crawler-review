from datetime import datetime, timedelta


def compute_next_retry_at(
    *,
    now: datetime,
    attempt_number: int,
    base_backoff_seconds: int,
    max_backoff_seconds: int,
    retry_after_seconds: int | None,
) -> datetime:
    if retry_after_seconds is not None:
        return now + timedelta(seconds=retry_after_seconds)

    backoff = min(
        max_backoff_seconds,
        base_backoff_seconds * (2 ** max(0, attempt_number - 1)),
    )
    return now + timedelta(seconds=backoff)
