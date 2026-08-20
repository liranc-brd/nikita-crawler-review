from __future__ import annotations

import httpx
from pydantic import BaseModel


class FetchResponse(BaseModel):
    status_code: int
    headers: dict[str, str]
    body: bytes | None = None


class FetchClient:
    def __init__(self, http_client: httpx.AsyncClient) -> None:
        self._http_client = http_client

    async def fetch(self, url: str) -> FetchResponse:
        response = await self._http_client.get("/fetch", params={"url": url})
        return FetchResponse.model_validate(response.json())
