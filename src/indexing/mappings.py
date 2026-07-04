"""
ScholarAssist — OpenSearch Mappings & Settings

Defines the OpenSearch index settings (analyzers, tokenizers, shards)
and the field mappings for the Golden Record schema.
"""

from typing import Any


def get_index_settings(number_of_shards: int = 3, number_of_replicas: int = 1) -> dict[str, Any]:
    """
    Returns the index settings including custom analyzers for academic text.
    """
    return {
        "index": {
            "number_of_shards": number_of_shards,
            "number_of_replicas": number_of_replicas,
            "refresh_interval": "30s",  # Optimised for bulk ingestion
        },
        "analysis": {
            "analyzer": {
                "academic_text": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": ["lowercase", "asciifolding", "stop", "snowball"],
                },
                "author_name": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": ["lowercase", "asciifolding"],
                },
                "exact_match": {
                    "type": "custom",
                    "tokenizer": "keyword",
                    "filter": ["lowercase"],
                },
            }
        },
    }


def get_index_mappings() -> dict[str, Any]:
    """
    Returns the OpenSearch field mappings for the ScholarAssist Golden Record.
    """
    return {
        "dynamic": "strict",
        "properties": {
            # Identifiers
            "golden_record_id": {"type": "keyword"},
            "doi": {"type": "keyword"},
            
            # Text fields
            "title": {
                "type": "text",
                "analyzer": "academic_text",
                "fields": {
                    "keyword": {"type": "keyword", "ignore_above": 256}
                }
            },
            "abstract": {
                "type": "text",
                "analyzer": "academic_text"
            },
            
            # Authors
            "authors": {
                "type": "nested",
                "properties": {
                    "id": {"type": "keyword"},
                    "name": {
                        "type": "text",
                        "analyzer": "author_name",
                        "fields": {
                            "keyword": {"type": "keyword", "ignore_above": 256}
                        }
                    },
                    "orcid": {"type": "keyword"},
                    "affiliations": {
                        "type": "text",
                        "analyzer": "standard",
                        "fields": {
                            "keyword": {"type": "keyword", "ignore_above": 256}
                        }
                    }
                }
            },
            
            # Venue
            "venue": {
                "properties": {
                    "name": {
                        "type": "text",
                        "fields": {
                            "keyword": {"type": "keyword", "ignore_above": 256}
                        }
                    },
                    "issn": {"type": "keyword"},
                    "type": {"type": "keyword"}
                }
            },
            
            # Metadata
            "publication_year": {"type": "integer"},
            "citation_count": {"type": "integer"},
            "references": {"type": "keyword"}, # Array of DOIs/IDs
            
            # Open Access
            "open_access": {
                "properties": {
                    "is_oa": {"type": "boolean"},
                    "oa_url": {"type": "keyword"}
                }
            },
            
            # Provenance
            "source_provenance": {
                "type": "object",
                "dynamic": True # Allow arbitrary field -> source mappings
            },
            
            # List of merged provider IDs
            "merged_provider_ids": {
                "type": "keyword"
            }
        }
    }


def get_index_body(number_of_shards: int = 3, number_of_replicas: int = 1) -> dict[str, Any]:
    """
    Combines settings and mappings into the full request body for index creation.
    """
    return {
        "settings": get_index_settings(number_of_shards, number_of_replicas),
        "mappings": get_index_mappings()
    }
