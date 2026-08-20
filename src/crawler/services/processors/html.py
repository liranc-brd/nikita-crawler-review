from __future__ import annotations

from typing import Any

from bs4 import BeautifulSoup


class HtmlProcessor:
    content_types = ("text/html",)
    metadata_type = "html"

    def extract_metadata(self, body: bytes, headers: dict[str, str]) -> dict[str, Any]:
        soup = BeautifulSoup(body, "html.parser")
        links = soup.find_all("a", href=True)
        return {
            "title": soup.title.string if soup.title else None,
            "discovered_link_count": len(links),
        }
