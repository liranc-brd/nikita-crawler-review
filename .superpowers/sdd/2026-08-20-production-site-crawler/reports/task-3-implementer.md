# Task 3 Implementer Report

## What I implemented

- Added `crawler.domain.types` as the domain-facing export for `JobStatus` and `UrlStatus`.
- Added URL normalization using `urljoin` for relative links, lowercased scheme/authority, default `/` paths, and fragment removal.
- Added strict exact-hostname comparison.
- Added `path_prefix` child-rule matching against URL paths.
- Added retry scheduling with `Retry-After` precedence, exponential backoff, and a maximum cap.
- Added focused unit coverage for the required examples and retry behavior.

## TDD evidence

### RED

Command:

```text
venv/bin/pytest tests/unit/test_url_policy.py tests/unit/test_retry_policy.py -v
```

Result: collection failed as expected because `crawler.domain` and its requested functions did not yet exist (`ModuleNotFoundError: No module named 'crawler.domain'`). No production implementation existed before this run.

### GREEN

Command:

```text
venv/bin/pytest tests/unit/test_url_policy.py tests/unit/test_retry_policy.py -v
```

Result: `7 passed`.

## Tests and results

- `venv/bin/pytest tests/unit/test_url_policy.py tests/unit/test_retry_policy.py -v`: 7 passed.
- `venv/bin/pytest tests/test_bootstrap.py -v`: 2 passed.
- `venv/bin/pytest tests/integration/test_job_creation.py -v`: 4 errors during setup because `DATABASE_URL` and `RABBITMQ_URL` are not set in the environment; all failures occur while constructing `Settings`, before Task 3 code is exercised.
- `venv/bin/pytest -v`: 9 passed, 4 integration setup errors for the same missing environment variables.

## Files changed

- `src/crawler/domain/__init__.py`
- `src/crawler/domain/types.py`
- `src/crawler/domain/url_policy.py`
- `src/crawler/domain/retry_policy.py`
- `tests/unit/test_url_policy.py`
- `tests/unit/test_retry_policy.py`
- This report file.

## Self-review findings

No Task 3 correctness or scope findings. The implementation preserves strict hostname equality and ignores unsupported child-rule kinds.

## Issues or concerns

The required integration and full-suite commands remain environment-blocked by missing `DATABASE_URL` and `RABBITMQ_URL`. No changes were made to unrelated generated caches or existing untracked directories.
