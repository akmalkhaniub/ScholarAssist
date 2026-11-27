"""
ScholarAssist — API Dependencies

FastAPI dependency injection providers.
"""
from typing import TYPE_CHECKING, Generator
from urllib.parse import urlparse

from src.config.settings import get_settings

if TYPE_CHECKING:
    from opensearchpy import OpenSearch


def parse_opensearch_host(url: str) -> dict[str, object]:
    """Parse an OpenSearch URL into host/port/ssl fields safely."""
    normalized = url if "://" in url else f"http://{url}"
    parsed = urlparse(normalized)
    host = parsed.hostname or "localhost"
    default_port = 443 if parsed.scheme == "https" else 9200
    port = parsed.port or default_port
    return {"host": host, "port": port, "use_ssl": parsed.scheme == "https"}


def get_opensearch_client() -> Generator["OpenSearch", None, None]:
    """Dependency that yields a configured OpenSearch client."""
    from opensearchpy import OpenSearch

    settings = get_settings().opensearch
    parsed_host = parse_opensearch_host(settings.url)

    kwargs = {
        "hosts": [{"host": parsed_host["host"], "port": parsed_host["port"]}],
        "use_ssl": settings.use_ssl or bool(parsed_host["use_ssl"]),
        "verify_certs": settings.verify_certs,
        "timeout": settings.timeout,
    }
    
    if settings.username and settings.password:
        kwargs["http_auth"] = (settings.username, settings.password)
        
    client = OpenSearch(**kwargs)
    try:
        yield client
    finally:
        client.close()
