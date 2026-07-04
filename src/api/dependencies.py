"""
ScholarAssist — API Dependencies

FastAPI dependency injection providers.
"""
from typing import Generator

from opensearchpy import OpenSearch

from src.config.settings import get_settings

def get_opensearch_client() -> Generator[OpenSearch, None, None]:
    """Dependency that yields a configured OpenSearch client."""
    settings = get_settings().opensearch
    
    kwargs = {
        "hosts": [{"host": settings.url.replace("http://", "").replace("https://", "").split(":")[0], 
                   "port": int(settings.url.split(":")[-1]) if ":" in settings.url else 9200}],
        "use_ssl": settings.use_ssl,
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
