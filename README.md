# Crawler

Production site crawler runtime. PostgreSQL is the durable source of crawl state;
RabbitMQ provides durable worker wake-ups and the scheduler republishes runnable
work after missed messages or expired leases.

## Prerequisites

- Python 3.12 or later
- Docker with Docker Compose
- An HTTP fetch service that implements `GET /fetch?url=<encoded-url>` and returns
  a JSON response with `status_code`, `headers`, and an optional `body`

## Local development

Create a virtual environment, install the project, and configure the runtime:

```bash
python3.12 -m venv venv
venv/bin/pip install -e .
cp .env.example .env
```

Set `FETCH_SERVICE_BASE_URL` in `.env` to the fetch service used by your
environment. The example uses the design's mock endpoint; it must not point at
the crawler API itself.

Start PostgreSQL and RabbitMQ, then wait for both services to become healthy:

```bash
docker compose up -d postgres rabbitmq
docker compose ps
```

Apply the database migrations:

```bash
venv/bin/alembic upgrade head
```

Run each long-lived process in its own terminal:

```bash
venv/bin/uvicorn crawler.main:create_app --factory --reload
```

```bash
venv/bin/python -m crawler.workers.crawler_worker
```

```bash
venv/bin/python -m crawler.workers.scheduler_worker
```

The API health endpoint is available at `http://localhost:8000/health`, and
RabbitMQ management is available at `http://localhost:15672`.

## Environment

`.env.example` defines every supported runtime setting. `DATABASE_URL` and
`RABBITMQ_URL` must point to PostgreSQL and RabbitMQ respectively. The remaining
settings control the fetch service, artifact root, crawl batch size, lease
heartbeat, lease lifetime, and scheduler polling interval.

Keep `HEARTBEAT_INTERVAL_SECONDS` shorter than `LEASE_DURATION_SECONDS` so a
healthy worker can renew its active URL lease before it expires.

## API operations

Create a crawl job:

```bash
curl -X POST http://localhost:8000/crawls \
  -H 'content-type: application/json' \
  -d '{"seed_url":"https://example.com","child_rules":[]}'
```

Use the returned crawl ID to inspect and control the job:

```bash
curl http://localhost:8000/crawls/$CRAWL_ID
curl http://localhost:8000/crawls/$CRAWL_ID/urls
curl http://localhost:8000/crawls/$CRAWL_ID/attempts
curl -X POST http://localhost:8000/crawls/$CRAWL_ID/pause
curl -X POST http://localhost:8000/crawls/$CRAWL_ID/resume
curl -X POST http://localhost:8000/crawls/$CRAWL_ID/cancel
```

## Operational checklist

- `docker compose ps` reports healthy `postgres` and `rabbitmq` services.
- `venv/bin/alembic upgrade head` completes before the API or workers start.
- `curl http://localhost:8000/health` returns `{"status":"ok"}`.
- The crawler worker and scheduler worker remain running without connection errors.
- A `POST /crawls` request returns `201` and a `pending` or `running` crawl status.
- `GET /crawls/$CRAWL_ID` reaches a terminal status after the configured fetch
  service processes all reachable URLs; inspect `/urls` and `/attempts` for failures.
- Restarting RabbitMQ or a worker does not discard durable crawl state; the
  scheduler reconciles runnable URLs and republishes their wake-ups.

## Verification

With PostgreSQL and RabbitMQ running and the configured environment loaded:

```bash
venv/bin/pytest tests/unit -v
venv/bin/pytest tests/integration -v
venv/bin/alembic upgrade head
venv/bin/python -c "from crawler.main import create_app; app = create_app(); print(app.routes)"
```
