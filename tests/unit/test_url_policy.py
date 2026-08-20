from crawler.domain.url_policy import (
    is_same_hostname,
    normalize_url,
    should_spawn_child,
)


def test_normalize_url_resolves_relative_links() -> None:
    assert normalize_url("/docs?page=1", "https://example.com/base") == "https://example.com/docs?page=1"


def test_normalize_url_lowercases_authority_and_drops_fragment() -> None:
    assert normalize_url("HTTPS://EXAMPLE.COM/Docs#section") == "https://example.com/Docs"


def test_is_same_hostname_is_strict() -> None:
    assert is_same_hostname("example.com", "https://example.com/about") is True
    assert is_same_hostname("example.com", "https://www.example.com/about") is False


def test_should_spawn_child_matches_prefix_rule() -> None:
    rules = [{"kind": "path_prefix", "value": "/products"}]
    assert should_spawn_child("https://example.com/products/42", rules) is True
    assert should_spawn_child("https://example.com/blog/42", rules) is False


def test_should_spawn_child_matches_regex_rule() -> None:
    rules = [{"kind": "regex", "value": r"^/products/\d+$"}]
    assert should_spawn_child("https://example.com/products/42", rules) is True
    assert should_spawn_child("https://example.com/products/featured", rules) is False
