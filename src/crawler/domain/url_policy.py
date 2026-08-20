import urllib.parse


def normalize_url(raw_url: str, base_url: str | None = None) -> str:
    resolved = urllib.parse.urljoin(base_url, raw_url) if base_url else raw_url
    parsed = urllib.parse.urlsplit(resolved)
    return urllib.parse.urlunsplit(
        (parsed.scheme.lower(), parsed.netloc.lower(), parsed.path or "/", parsed.query, "")
    )


def is_same_hostname(seed_hostname: str, candidate_url: str) -> bool:
    return urllib.parse.urlsplit(candidate_url).hostname == seed_hostname


def should_spawn_child(url: str, child_rules: list[dict[str, str]]) -> bool:
    path = urllib.parse.urlsplit(url).path or "/"
    return any(
        rule.get("kind") == "path_prefix" and path.startswith(rule.get("value", ""))
        for rule in child_rules
    )
