# ScholarAssist — Academic Dataset Pipeline

Production-grade academic dataset pipeline powering ScholarAssist's citation and verification platform.

## Architecture

This system implements a **Medallion Data Lake Architecture** (Bronze → Silver → Gold) to ingest, normalize, deduplicate, index, and serve **300M+ academic records** via a REST API.

```
Data Sources → Bulk/Incremental Ingestion → Raw Storage (Bronze)
    → Spark Normalization (Silver) → Entity Resolution & Dedup (Gold)
    → OpenSearch Indexing → FastAPI REST API
```

## Data Sources

| Source | Records | License | Ingestion Method |
|--------|---------|---------|-----------------|
| OpenAlex | ~250M | CC0 | S3 bulk dump + REST API |
| Crossref | ~150M | CC0 (metadata) | Public data file + REST API |
| Semantic Scholar | ~200M | Research license | S3 requester-pays + Graph API |
| CORE | ~200M | Mixed | Bulk dump + REST API |
| Unpaywall | ~30M | CC0 | Data feed + REST API |
| DBLP | ~7M | CC0 | XML dump + REST API |
| OpenCitations | ~1.5B links | CC0 | CSV dump + REST API |

## Quick Start (Local Development)

### Prerequisites
- Docker & Docker Compose
- Python 3.11+
- Make

### Setup

```bash
# Clone and enter the project
cd scholarassist

# Copy environment variables
cp .env.example .env

# Start the full local dev stack
make up

# Download sample data for development
make seed
```

### Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| **API (Swagger)** | http://localhost:8000/docs | — |
| **MinIO Console** | http://localhost:9001 | minioadmin / minioadmin |
| **OpenSearch** | http://localhost:9200 | — |
| **OpenSearch Dashboards** | http://localhost:5601 | — |
| **Spark Master UI** | http://localhost:8081 | — |
| **Airflow UI** | http://localhost:8080 | admin / admin |

### Common Commands

```bash
make up              # Start all services
make down            # Stop all services
make logs            # Tail all logs
make test            # Run test suite
make lint            # Run linters
make seed            # Download samples and seed OpenSearch
make clean           # Remove containers and caches
```

## Project Structure

```
scholarassist/
├── src/
│   ├── connectors/      # Data source connectors (OpenAlex, Crossref, etc.)
│   ├── ingestion/       # Bulk & incremental ingestion pipeline
│   ├── normalization/   # Spark-based schema normalization
│   ├── deduplication/   # Entity resolution & Golden Record generation
│   ├── indexing/        # OpenSearch index management
│   ├── api/             # FastAPI REST API
│   ├── quality/         # Data quality & audit reporting
│   └── config/          # Application configuration
├── dags/                # Airflow DAG definitions
├── tests/               # Test suite
├── scripts/             # Utility scripts
├── terraform/           # AWS infrastructure (production)
└── docs/                # Documentation
```

## Technology Stack

- **Processing**: Apache Spark (PySpark) + Delta Lake
- **Storage**: S3 / MinIO (Medallion architecture)
- **Search**: OpenSearch
- **API**: FastAPI
- **Orchestration**: Apache Airflow
- **Containers**: Docker + Docker Compose (dev) / Kubernetes (prod)
- **IaC**: Terraform (AWS)

## License

Proprietary — ScholarAssist
