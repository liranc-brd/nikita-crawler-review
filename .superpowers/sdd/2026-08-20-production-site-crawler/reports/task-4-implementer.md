# Task 4 Implementer Report

## Status

Completed `Build Repository And Transactional Crawl-State Operations` on
`feat-production-site-crawler`.

## Implemented

- Added `JobRepository` with normalized job creation, pause/resume/cancel
  requests, recursive child cancellation, lifecycle advancement, and drained
  job completion.
- Added `UrlRepository` with conflict-safe seed insertion, transactional
  `FOR UPDATE SKIP LOCKED` claims, explicit worker-bound heartbeats, expired
  lease recovery, and retry-wait state persistence.
- Added `DiscoveryRepository` for normalized discovery audit rows.
- Added `LifecycleAdvanceResult` for scheduler-facing lifecycle counts.
- Added PostgreSQL integration coverage for repository creation and seeding,
  two-session lock contention, retry eligibility, lease recovery, heartbeat
  ownership, retry transitions, lifecycle controls, completion, and external
  discovery audit rows.
- Added fixture cleanup using unique `task4-*.example.com` hostnames so the
  integration tests do not leave durable test rows behind.

The pre-existing `async_session_factory` already creates independent
`AsyncSession` instances from a shared engine, which is the required session
behavior for the separate-session locking test. No change to
`src/crawler/db/session.py` was necessary.

## TDD Evidence

### RED

1. `DATABASE_URL=postgresql+asyncpg://crawler:crawler@localhost:5432/crawler RABBITMQ_URL=amqp://guest:guest@localhost:5672/ venv/bin/pytest tests/integration/test_claiming_and_leases.py tests/integration/test_pause_resume_cancel.py -v`
   - Failed during collection as expected with
     `ModuleNotFoundError: No module named 'crawler.repos'`.
2. `DATABASE_URL=... RABBITMQ_URL=... venv/bin/pytest tests/integration/test_claiming_and_leases.py::test_create_job_uses_hostname_without_port -v`
   - Failed as expected: `seed_hostname` was `example.com:8443` instead of
     `example.com`.

### GREEN

1. `DATABASE_URL=... RABBITMQ_URL=... venv/bin/pytest tests/integration/test_claiming_and_leases.py::test_claim_runnable_urls_allows_only_one_active_worker -v`
   - Passed: `1 passed` using two independent database sessions.
2. `DATABASE_URL=... RABBITMQ_URL=... venv/bin/pytest tests/integration/test_claiming_and_leases.py::test_create_job_uses_hostname_without_port -v`
   - Passed: `1 passed` after hostname parsing was corrected.
3. `DATABASE_URL=... RABBITMQ_URL=... venv/bin/pytest tests/integration/test_claiming_and_leases.py tests/integration/test_pause_resume_cancel.py -v`
   - Passed: `13 passed`.

## Verification

- `venv/bin/python -m compileall -q src` exited successfully.
- `DATABASE_URL=... RABBITMQ_URL=... venv/bin/pytest tests/integration/test_job_creation.py -v`
  passed: `4 passed`.
- `DATABASE_URL=... RABBITMQ_URL=... venv/bin/pytest -v`
  passed: `27 passed`.

Integration commands used the configured PostgreSQL and RabbitMQ URLs. The
first sandboxed database test was blocked by the sandbox's localhost socket
policy; the same commands were rerun with approved local-database access.

## Files Changed

- `src/crawler/repos/__init__.py`
- `src/crawler/repos/jobs.py`
- `src/crawler/repos/urls.py`
- `src/crawler/repos/discoveries.py`
- `tests/integration/test_claiming_and_leases.py`
- `tests/integration/test_pause_resume_cancel.py`
- `.superpowers/sdd/2026-08-20-production-site-crawler/reports/task-4-implementer.md`

## Self-Review

- No production-code findings remain.
- Claims use `FOR UPDATE SKIP LOCKED` and flush their ownership updates before
  another session can claim the same row.
- Heartbeats require both the current worker identity and an active URL state.
- Lifecycle transitions wait for active URL states before making pause/cancel
  terminal, while cancellation immediately prevents unclaimed work.
- URL seeding is normalized and idempotent through the database uniqueness
  constraint.

## Issues Or Concerns

None.

## Fix Round 1

### Implemented

- Narrowed the claim lock to `CrawlUrl` with PostgreSQL-equivalent
  `FOR UPDATE OF crawl_urls SKIP LOCKED`, allowing independent URL rows from
  the same job to be claimed concurrently.
- Guarded `mark_retry_wait` by `url_id`, `worker_id`, and active URL state.
  It now returns `True` only when the guarded transition succeeds, preventing
  stale workers from overwriting reclaimed work.
- Updated lifecycle advancement to move every non-active, non-terminal URL
  for `canceling` jobs to `canceled` before finalizing a drained job as
  `canceled`. This includes URLs released after their lease expires.

### Regression Coverage

- Two separate sessions claim distinct URLs in the same job concurrently.
- A stale worker cannot put a URL back into retry wait after the lease is
  released and another worker has claimed it.
- A canceling job with an expired claim finalizes both the recovered URL and
  the job as `canceled`.

### TDD Evidence

RED command:

`DATABASE_URL=postgresql+asyncpg://crawler:crawler@localhost:5432/crawler RABBITMQ_URL=amqp://guest:guest@localhost:5672/ venv/bin/pytest tests/integration/test_claiming_and_leases.py::test_claim_runnable_urls_allows_workers_to_claim_distinct_urls_in_one_job tests/integration/test_claiming_and_leases.py::test_mark_retry_wait_rejects_a_stale_worker_after_reclaim tests/integration/test_pause_resume_cancel.py::test_cancel_finalizes_a_url_recovered_after_cancel_request -v`

Result: all three tests failed as expected. The second worker could not claim
the second URL, `mark_retry_wait` did not accept a worker ID, and the recovered
URL remained `queued` after its job became `canceled`.

GREEN verification:

- Focused two-worker/two-URL claim regression: `1 passed`.
- Focused retry ownership regressions: `2 passed`.
- Focused cancellation and lease-recovery regressions: `2 passed`.
- Covering Task 4 integration suite: `16 passed`.
- Full project suite: `30 passed`.

### Concerns

None.

### Commit

Fix Round 2 implementation committed as `820b813 fix: require terminal frontier before cancellation`.

### Commit

Fix Round 1 implementation committed as `4047b32 fix: harden crawl state transitions`.

## Fix Round 2

### Implemented

- Changed `CANCELING -> CANCELED` finalization to require no non-terminal URL
  rows. The terminal-frontier predicate is shared with drained-job completion.
- Kept the existing cancellation frontier update, claim lock narrowing, and
  stale-worker retry ownership guard unchanged.

### Regression Coverage

- Added a real two-session integration regression that releases an expired
  lease after cancellation's frontier update and before its final job update.
  The job remains `canceling` and the recovered URL remains `queued`, rather
  than producing a canceled job with queued work.

### TDD Evidence

RED command:

`DATABASE_URL=postgresql+asyncpg://crawler:crawler@localhost:5432/crawler RABBITMQ_URL=amqp://guest:guest@localhost:5672/ venv/bin/pytest tests/integration/test_pause_resume_cancel.py::test_cancel_does_not_finalize_when_a_lease_recovers_during_advancement -v`

Result: failed as expected with `canceled_jobs == 1`; the old finalization
predicate ignored the recovered queued URL.

GREEN verification:

- Focused cancellation race regression: `1 passed`.
- Covering Task 4 integration suite: `17 passed`.
- Full project suite: `31 passed`.

### Concerns

None.
