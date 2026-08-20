# Production Site Crawler Design

## Overview

This document describes a production-grade site crawler that starts from a seed URL, discovers and downloads same-site content, processes supported content types, persists raw artifacts and metadata, and exposes operational controls for multi-job crawling.

The system must support:

- Multiple concurrent crawl jobs across many sites
- Independent job progress, retries, and resumability
- Strict crawl boundaries limited to the seed hostname by default
- Optional child crawl jobs for discovered URLs matching configured rules
- Durable state, observability, and operator controls

The design uses PostgreSQL as the source of truth for crawl state and RabbitMQ only as a delivery and wake-up mechanism.

## Goals

- Accept a seed URL and crawl content reachable from it within the allowed boundary
- Persist durable crawl state so jobs can be resumed after process or machine failure
- Guarantee each normalized URL has at most one active worker at a time per crawl job, even under concurrency
- Download, process, and persist HTML, images, videos, and PDFs
- Expose API endpoints for lifecycle control and inspection
- Keep content-type processing extensible so adding a new type does not require rewriting the crawler core

## Non-Goals

- Implementing the external fetch service
- Crawling across multiple hostnames by default
- Full browser rendering or JavaScript execution
- Internet-scale crawling or cross-job global URL deduplication

## Requirements And Constraints

### Fetch Dependency

All content retrieval goes through the external fetch API:

`GET http://mock-api.mock.com/fetch?url=<encoded_url>`

This dependency must be treated as unreliable and non-deterministic. Responses may vary between attempts. The crawler must use response headers deliberately rather than inferring content type from URL structure.

Supported response classes:

- `200`: success
- `404`: not found
- `403`: blocked
- `429`: rate limited
- `500`: temporary server error

### Content Handling

The crawler must persist raw downloaded content into:

- `output/html/`
- `output/images/`
- `output/videos/`
- `output/pdfs/`

It must also extract lightweight metadata:

- HTML: page title and discovered link count
- Images: width, height, and file size
- Videos: file size and duration if available
- PDFs: page count and document title if present

### Crawl Boundary

The default crawl boundary is strict to the exact seed hostname.

Rules:

- Resolve relative URLs against the current page URL before evaluation
- Normalize URLs before deduplication or policy checks
- Compare discovered URLs against the original seed hostname
- Only exact seed-hostname matches are eligible by default
- External hostnames must be ignored completely

Example with seed `https://example.com`:

- Allowed: `https://example.com/page`
- Allowed: `https://example.com/products/123`
- Ignored: `https://google.com`
- Ignored: `https://youtube.com`
- Ignored: `https://external.example.org`

The hostname rule can be made configurable later, but the default behavior must remain strict and explicit.

### Child Crawl Jobs

Discovered URLs on the allowed hostname may either:

- Stay in the current crawl job, or
- Spawn a child crawl job if they match configured child-job rules

Child jobs are created only for configured path prefixes or regex-like patterns. URLs that do not match a child-job rule remain in the current job by default.

If a discovered URL spawns a child job, the parent job should not also process that URL locally.

## Architecture

The system is split into four runtime components:

1. API service
2. Crawler worker
3. Scheduler/control worker
4. PostgreSQL and RabbitMQ infrastructure

### API Service

The API service is responsible for:

- Creating crawl jobs
- Returning job status and progress
- Pausing, resuming, and canceling jobs
- Exposing inspection endpoints for discovered URLs, retries, and job relationships

The API writes durable state to PostgreSQL first, then publishes a RabbitMQ wake-up message after commit.

### Crawler Worker

The crawler worker consumes wake-up messages from RabbitMQ but never trusts the queue as the source of truth. Instead, it claims runnable work from PostgreSQL transactionally, fetches content, persists artifacts, invokes the appropriate content processor, records metadata, discovers links, and schedules new work or child jobs.

### Scheduler/Control Worker

The scheduler/control worker is a separate lightweight role with these responsibilities:

- Re-enqueue runnable work whose retry delay has expired
- Requeue work after jobs are resumed
- Recover stale claims from dead workers
- Advance jobs between transitional and stable states

Keeping this as a separate role isolates timing and control concerns from fetch and processing throughput.

### PostgreSQL

PostgreSQL is the source of truth for:

- Crawl jobs
- URL frontier and visited state
- Fetch attempts and retry timing
- Discovery relationships
- Content artifact records
- Extracted metadata
- Parent/child job relationships

Every operator-visible answer should be derivable from PostgreSQL without reconstructing queue history.

The scheduler must also treat PostgreSQL as the recovery source if RabbitMQ delivery fails. Periodic reconciliation scans must detect runnable work in PostgreSQL and publish fresh wake-up messages even if earlier messages were lost, delayed indefinitely, or never published.

### RabbitMQ

RabbitMQ is a wake-up and delivery mechanism only.

Messages may be duplicated or delayed without violating correctness because workers always re-check PostgreSQL before claiming or performing work.

## Core Data Model

### `crawl_jobs`

One row per crawl job.

Key fields:

- `id`
- `seed_url`
- `seed_hostname`
- `parent_job_id`
- `status`
- `config`
- `created_at`
- `started_at`
- `finished_at`
- `pause_requested_at`
- `cancel_requested_at`

`config` stores per-job settings such as:

- Worker concurrency cap
- Retry policy
- Child-job rules
- Supported content-type policy
- Optional crawl limits like maximum depth or maximum pages

Child jobs reference their parent via `parent_job_id` and store their own configuration snapshot at creation time.

### `crawl_urls`

One row per normalized URL within a specific crawl job.

Key fields:

- `id`
- `job_id`
- `normalized_url`
- `url_hash`
- `discovered_from_url_id`
- `status`
- `content_type`
- `http_status_code`
- `fetch_attempts`
- `next_eligible_at`
- `claimed_by`
- `claimed_at`
- `started_at`
- `finished_at`
- `content_artifact_id`
- `error_code`
- `error_detail`

This table represents both the crawl frontier and the visited set.

Uniqueness constraint:

- `(job_id, normalized_url)` or equivalent hash-backed uniqueness

That constraint is the foundation for per-job URL deduplication and for ensuring that multiple workers do not create parallel active executions for the same URL in one job.

### `crawl_attempts`

Append-only fetch-attempt history.

Key fields:

- `id`
- `crawl_url_id`
- `attempt_number`
- `started_at`
- `finished_at`
- `result_status`
- `http_status_code`
- `retry_after_seconds`
- `response_headers`
- `error_detail`

This table supports retry inspection and postmortem analysis without overloading current-state rows.

### `discovered_links`

Graph and audit table for link discovery.

Key fields:

- `id`
- `job_id`
- `source_url_id`
- `target_normalized_url`
- `target_url_id`
- `is_same_hostname`
- `spawned_child_job_id`
- `discovered_at`

This table is useful for read-only inspection endpoints and explaining why a link was ignored, enqueued, or turned into a child job.

### `content_artifacts`

Tracks persisted raw files.

Key fields:

- `id`
- `job_id`
- `crawl_url_id`
- `content_type`
- `storage_path`
- `filename`
- `content_length`
- `content_hash`
- `etag`
- `last_modified`
- `saved_at`

This creates a durable trace between database records and files on disk.

### `content_metadata`

Processor output table.

Key fields:

- `id`
- `artifact_id`
- `metadata_type`
- `metadata_json`

Examples:

- HTML: title and discovered link count
- Image: width, height, file size
- Video: file size, duration if available
- PDF: page count, title if available

This structure keeps content processors extensible. A new processor can write the same artifact plus metadata contract without changing crawler orchestration tables.

## State Machines

### Crawl Job States

`crawl_jobs.status`:

- `pending`
- `running`
- `pausing`
- `paused`
- `canceling`
- `canceled`
- `completed`
- `failed`

Transitions:

- `pending -> running`
- `running -> pausing -> paused`
- `paused -> running`
- `running|paused|pausing -> canceling -> canceled`
- `running -> completed`
- `running -> failed` only for job-scope unrecoverable errors

Ordinary URL-level failures should not fail the whole job.

### Crawl URL States

`crawl_urls.status`:

- `discovered`
- `queued`
- `claimed`
- `fetching`
- `processing`
- `retry_wait`
- `done`
- `failed_permanent`
- `canceled`
- `skipped_child_spawned`

Status handling:

- `200` leads to processing
- `404` becomes `failed_permanent`
- `403` becomes `failed_permanent` with a blocked reason
- `429` becomes `retry_wait`
- `500` becomes `retry_wait`
- Dependency or transport failures become transient unless retry limits are exhausted

## Concurrency And Idempotency

RabbitMQ messages are advisory. PostgreSQL authorizes work.

Workers must claim rows from PostgreSQL in a transaction using a skip-locked pattern. A claim query filters for:

- Jobs in `running`
- URLs in `queued` or `retry_wait`
- `next_eligible_at <= now()`
- Not already claimed

Claiming a row atomically sets:

- `status = claimed`
- `claimed_by`
- `claimed_at`

This ensures:

- At most one active worker per URL row
- Safe horizontal scaling across many workers
- Safe handling of duplicate queue deliveries

Discovered URLs must be inserted idempotently using conflict-aware writes. Repeated discovery of the same normalized URL inside one job must not create duplicates.

Artifacts should use deterministic naming derived from normalized URL and content hash or URL hash. This preserves traceability and reduces duplicate file creation after partial failures.

If a worker crashes after claiming work, the scheduler must detect stale claims and return those rows to a runnable state.

This is intentionally not a true exactly-once or true at-most-once processing guarantee. A URL may be retried or reprocessed after worker death, lease expiry, or ambiguous downstream failure. The correctness guarantee is narrower and operationally safer: only one worker may actively own a given URL row at a time.

### Worker Lease And Heartbeat Handling

Claimed work must use an explicit lease model rather than a single static timeout.

Recommended fields on `crawl_urls`:

- `lease_expires_at`
- `last_heartbeat_at`

Worker behavior:

- When claiming a URL, set an initial lease expiry
- While the worker is actively fetching, persisting, or processing, periodically heartbeat to extend the lease
- Heartbeats should happen on a short interval relative to the lease duration
- Lease extension must be conditional on the row still being owned by the same worker

Scheduler behavior:

- Treat a URL as abandoned only when its lease has expired and no recent heartbeat has been observed
- Requeue abandoned work by clearing ownership fields and moving the row back to `queued` or `retry_wait`, depending on the last known safe state
- Record lease recovery events for audit and troubleshooting

Long-running tasks such as large video downloads or heavy PDF inspection must rely on heartbeats rather than oversized fixed leases. This reduces duplicate recovery during healthy long work while still allowing fast recovery from dead workers.

## Retry Semantics

Retry behavior must distinguish transient from permanent failures.

Recommended policy:

- `200`: success, no retry
- `404`: permanent failure
- `403`: permanent failure by default
- `429`: retry, honoring `Retry-After` when present
- `500`: retry with exponential backoff and jitter
- Dependency/network/parsing anomalies: retry as transient up to a configured cap

Per-job retry policy should include:

- `max_attempts`
- `base_backoff_seconds`
- `max_backoff_seconds`
- `respect_retry_after`

For transient failures:

- Append a `crawl_attempts` row
- Increment `fetch_attempts`
- Set `status = retry_wait`
- Compute `next_eligible_at`
- Persist the latest failure code and details

A URL reaches terminal failure only when:

- The response class is inherently permanent, or
- Retry attempts exceed configured policy

The scheduler is responsible for finding `retry_wait` rows whose `next_eligible_at` has passed and publishing wake-up messages.

The scheduler must also periodically reconcile all runnable states in PostgreSQL, including:

- `queued` rows that have not been claimed
- `retry_wait` rows whose eligibility time has arrived
- Rows released from expired leases
- Seed work for newly created or resumed jobs

This reconciliation loop is the safety net that guarantees progress even if RabbitMQ wake-up messages are lost.

## Child-Job Creation

Child-job creation occurs during HTML processing after link extraction.

For each discovered link:

1. Resolve relative URLs against the current page URL
2. Normalize
3. Reject non-HTTP(S) URLs
4. Reject URLs whose hostname differs from the original seed hostname
5. Record the discovery event
6. Evaluate child-job rules
7. Either enqueue in the current job or create/find a child job

Child-job rules are ordered and configuration-driven. Supported rule forms may include:

- Path prefix match
- Regex or equivalent structured pattern

Default action:

- Do not spawn a child job unless a rule matches

External-hostname links must never be enqueued or followed. They should still be recorded in `discovered_links` for audit and inspection, marked as `is_same_hostname = false`, unless operators later decide they want a strict no-audit mode. The current design favors observability over silent dropping.

Child-job creation must be idempotent. Multiple discoveries of the same child-triggering URL must not create duplicate child jobs. A uniqueness rule such as `(parent_job_id, seed_url)` should enforce this.

When a child job is created:

- The child gets its own `crawl_jobs` row
- The parent-child relationship is persisted
- The discovery edge references the spawned child job
- The matching URL is not inserted into the parent job frontier

## Pause, Resume, And Cancel Semantics

These controls are cooperative and state-driven.

### Pause

When pause is requested:

- The API sets the job to `pausing`
- Workers stop claiming new URLs for that job
- In-flight fetch or processing may finish
- The scheduler stops re-enqueuing work for that job

When no active URL rows remain, the job transitions to `paused`.

### Resume

When resume is requested:

- The API moves the job from `paused` to `running`
- The scheduler republishes wake-up messages for eligible runnable URLs

No queue reconstruction is needed because all frontier state is already in PostgreSQL.

### Cancel

When cancel is requested:

- The API sets the job to `canceling`
- Workers stop claiming new URLs immediately
- Workers check job state before expensive or state-expanding steps
- The scheduler stops re-enqueueing work

Remaining non-terminal URLs are eventually marked `canceled`. Once active work drains or is safely abandoned, the job becomes `canceled`.

Cancellation policy for descendants should be configurable. The recommended default is cascading cancellation to child jobs because parent and child crawls are operationally related.

## Parent And Child Completion Semantics

Parent-job completion must be defined independently from descendant execution unless explicitly configured otherwise.

Recommended default:

- A parent job is `completed` when its own URL frontier has reached terminal states and it has no runnable or active URL rows left
- Child jobs remain independently tracked and do not block the parent from reaching `completed`
- Parent status endpoints should expose aggregate child-job counts and descendant statuses so operators can still understand the full crawl tree

Optional future policy:

- A tree-aware completion mode could treat a root job as complete only when all descendants are also terminal

The default independent-completion model keeps lifecycle ownership clear and avoids a parent remaining artificially open because one descendant is delayed, paused, or retrying.

## Processing Pipeline

The worker pipeline for a claimed URL is:

1. Re-check job eligibility and URL state
2. Fetch through the external fetch API
3. Classify by response headers
4. Persist raw artifact if supported
5. Invoke the appropriate processor
6. Persist metadata
7. If HTML, extract links and schedule follow-up actions
8. Transition the URL to a terminal or retry state

Processing must be content-type driven, not URL-extension driven.

Each supported content type should have a dedicated processor behind a common interface, for example:

- `can_process(content_type)`
- `extract_metadata(body, headers)`

This keeps the crawler core closed to modification when new content handlers are added.

## API Surface

Write endpoints:

- `POST /crawls` to create a crawl
- `POST /crawls/{job_id}/pause`
- `POST /crawls/{job_id}/resume`
- `POST /crawls/{job_id}/cancel`

Read-only endpoints:

- `GET /crawls/{job_id}` for status and progress
- `GET /crawls/{job_id}/urls`
- `GET /crawls/{job_id}/attempts`
- `GET /crawls/{job_id}/children`
- `GET /crawls/{job_id}/parent`
- `GET /crawls/{job_id}/discoveries`

The status response should include aggregate progress such as:

- Total discovered
- Runnable
- In progress
- Retry waiting
- Done
- Permanently failed
- Canceled
- Child jobs spawned

## Observability

Important operations must emit structured logs with:

- Job ID
- URL ID
- Normalized URL
- Attempt number
- Worker identity
- State transition
- Failure reason where applicable

Metrics should cover:

- Jobs created, completed, paused, canceled
- URLs processed by state and content type
- Retry counts by response class
- Fetch latency
- Processor latency
- Queue wake-up volume
- Stale-claim recoveries

## Security And Robustness Considerations

- Reject unsupported URL schemes
- Normalize URLs consistently before policy checks or deduplication
- Do not rely on file extensions for content classification
- Preserve enough artifact and attempt metadata for debugging inconsistent upstream responses
- Avoid logging secrets or full sensitive payloads
- Treat disk writes and DB updates as coordinated but failure-prone operations

## Trade-Offs And Rationale

### Why PostgreSQL As The Source Of Truth

This design prioritizes correctness, resumability, and inspectability over minimal moving parts. PostgreSQL provides:

- Strong transactional deduplication
- Reliable state transitions under concurrency
- Queryable operational state
- Straightforward support for pause, resume, and cancellation

### Why RabbitMQ Only As A Wake-Up Mechanism

Queue-first designs make duplicate delivery, retries, and control semantics harder to reason about. By keeping RabbitMQ advisory, the system remains correct even if messages are duplicated, delayed, or lost.

### Why Separate Scheduler And Crawler Roles

Separating these roles improves failure isolation and keeps the crawler worker focused on fetch and processing throughput while the scheduler handles delayed eligibility, lease recovery, and lifecycle transitions.

## Open Implementation Decisions

The following should be resolved in the implementation plan:

- Exact URL normalization policy details
- Exact child-rule configuration schema
- Concrete processor libraries for image, video, and PDF metadata extraction
- File naming convention under `output/`
- Whether progress is computed live or maintained as cached aggregates
- Whether cancel always cascades to descendants or is per-request configurable

## Recommended Next Step

Create an implementation plan covering:

- Schema and migration files
- FastAPI endpoints
- Worker and scheduler responsibilities
- URL normalization and domain-policy utilities
- Processor interface and per-type handlers
- Retry logic and stale-claim recovery
- Tests for state transitions, deduplication, and child-job behavior
