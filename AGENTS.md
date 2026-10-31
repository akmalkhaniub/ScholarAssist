# AGENTS.md — Autonomous Agent Operating Contract

## 🎯 Repository Purpose & Architecture
`scholarassist` is an academic dataset ingestion and deduplication pipeline implementing a Medallion Data Lake architecture (Bronze -> Silver -> Gold) with Apache Spark, Airflow DAGs, and FastAPI semantic search APIs.

### Tech Stack
- **Pipeline Engine**: Apache Spark, Apache Airflow, Docker Compose
- **API**: FastAPI, Pydantic v2
- **Frontend**: Next.js + TypeScript

---

## ⚡ Autonomous Execution Protocol
1. Pull task from `TASKS.json`.
2. Adhere to idempotent ETL pipeline constraints.
3. Run verification: `pytest tests/`.
4. Commit with: `feat(task-id): description`.
