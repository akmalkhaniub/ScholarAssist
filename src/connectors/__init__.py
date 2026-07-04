# ScholarAssist — Data Source Connectors
"""
Convenience re-exports so callers can do::

    from src.connectors import OpenAlexConnector, CrossrefConnector, ...
"""

from src.connectors.base import (
    BaseConnector,
    IngestionManifest,
    IngestionMode,
    SourceName,
)
from src.connectors.openalex import OpenAlexConnector
from src.connectors.crossref import CrossrefConnector
from src.connectors.semantic_scholar import SemanticScholarConnector
from src.connectors.core_ac import COREConnector
from src.connectors.unpaywall import UnpaywallConnector
from src.connectors.dblp import DBLPConnector
from src.connectors.opencitations import OpenCitationsConnector

__all__ = [
    # Base
    "BaseConnector",
    "IngestionManifest",
    "IngestionMode",
    "SourceName",
    # Connectors
    "OpenAlexConnector",
    "CrossrefConnector",
    "SemanticScholarConnector",
    "COREConnector",
    "UnpaywallConnector",
    "DBLPConnector",
    "OpenCitationsConnector",
]
