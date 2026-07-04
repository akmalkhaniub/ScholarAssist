# ScholarAssist API Reference

## Authentication
Currently, the API is open for local development. In production, provide an API key in the `Authorization: Bearer <key>` header.

## Endpoints

### `GET /v1/records/search`
Search across all unified Golden Records.

**Query Parameters**:
- `q` (string, required): Full-text search query (Lucene syntax supported).
- `page` (int, default=1): Page number for pagination.
- `size` (int, default=20): Number of results per page (max 100).
- `year_start` (int, optional): Filter by publication year (gte).
- `year_end` (int, optional): Filter by publication year (lte).

**Response**: `SearchResponse`
```json
{
  "total_hits": 15432,
  "page": 1,
  "size": 20,
  "results": [
    {
      "golden_record_id": "gr_12345",
      "doi": "10.1234/abcd",
      "title": "Example Paper Title",
      "authors": [
        {"id": "a1", "name": "John Doe", "orcid": "0000-0000-0000-0000", "affiliations": ["University X"]}
      ],
      "publication_year": 2023,
      "venue": {"name": "Nature", "issn": "1234-5678", "type": "journal"},
      "citation_count": 42,
      "source_provenance": {"title": "crossref", "citation_count": "openalex"}
    }
  ]
}
```

### `GET /v1/records/{record_id}`
Retrieve a specific Golden Record by its ID.

**Path Parameters**:
- `record_id` (string): The Golden Record ID.

**Response**: `ScholarRecord`

### `GET /v1/health`
Check the health status of the API and its dependencies (OpenSearch, Redis).
