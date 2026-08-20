from fastapi import FastAPI

from crawler.logging import configure_logging


def create_app() -> FastAPI:
    configure_logging()
    app = FastAPI(title="Crawler")

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    return app


def run() -> None:
    import uvicorn

    uvicorn.run("crawler.main:create_app", factory=True)
