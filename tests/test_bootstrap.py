from crawler.config import Settings
from crawler.main import create_app


def test_create_app_exposes_health_route() -> None:
    app = create_app()
    routes = {route.path for route in app.routes}
    assert "/health" in routes


def test_settings_reads_required_environment_variables(monkeypatch) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://crawler:crawler@localhost:5432/crawler")
    monkeypatch.setenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
    settings = Settings()
    assert str(settings.database_url).startswith("postgresql+asyncpg://")
