import asyncio

from crawler.config import Settings
from crawler.main import create_app
from crawler.workers import crawler_worker, scheduler_worker


def test_create_app_exposes_health_route() -> None:
    app = create_app()
    routes = {route.path for route in app.routes}
    assert "/health" in routes


def test_settings_reads_required_environment_variables(monkeypatch) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://crawler:crawler@localhost:5432/crawler")
    monkeypatch.setenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
    settings = Settings()
    assert str(settings.database_url).startswith("postgresql+asyncpg://")


def test_crawler_worker_main_enters_runtime_loop(monkeypatch) -> None:
    called: dict[str, object] = {}

    async def fake_run_crawler_worker() -> None:
        called["entered"] = True

    def fake_asyncio_run(coro: object) -> None:
        called["called"] = True
        assert asyncio.iscoroutine(coro)
        coro.close()

    monkeypatch.setattr(crawler_worker, "run_crawler_worker", fake_run_crawler_worker)
    monkeypatch.setattr(crawler_worker.asyncio, "run", fake_asyncio_run)

    crawler_worker.main()

    assert called == {"called": True}


def test_scheduler_worker_main_enters_runtime_loop(monkeypatch) -> None:
    called: dict[str, object] = {}

    async def fake_run_scheduler_worker() -> None:
        called["entered"] = True

    def fake_asyncio_run(coro: object) -> None:
        called["called"] = True
        assert asyncio.iscoroutine(coro)
        coro.close()

    monkeypatch.setattr(scheduler_worker, "run_scheduler_worker", fake_run_scheduler_worker)
    monkeypatch.setattr(scheduler_worker.asyncio, "run", fake_asyncio_run)

    scheduler_worker.main()

    assert called == {"called": True}
