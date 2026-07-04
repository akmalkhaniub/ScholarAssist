"""
ScholarAssist — API Models (Records)
"""

from typing import Optional
from pydantic import BaseModel, Field


class AuthorModel(BaseModel):
    id: Optional[str] = None
    name: str
    orcid: Optional[str] = None
    affiliations: list[str] = Field(default_factory=list)


class VenueModel(BaseModel):
    name: Optional[str] = None
    issn: Optional[str] = None
    type: Optional[str] = None


class OpenAccessModel(BaseModel):
    is_oa: Optional[bool] = None
    oa_url: Optional[str] = None


class ScholarRecord(BaseModel):
    """The Golden Record returned by the API."""
    golden_record_id: str
    doi: Optional[str] = None
    title: Optional[str] = None
    authors: list[AuthorModel] = Field(default_factory=list)
    publication_year: Optional[int] = None
    venue: Optional[VenueModel] = None
    abstract: Optional[str] = None
    references: list[str] = Field(default_factory=list)
    citation_count: Optional[int] = None
    open_access: Optional[OpenAccessModel] = None
    source_provenance: dict[str, str] = Field(default_factory=dict)
    merged_provider_ids: list[str] = Field(default_factory=list)

    class Config:
        from_attributes = True


class SearchResponse(BaseModel):
    total_hits: int
    page: int
    size: int
    results: list[ScholarRecord]
