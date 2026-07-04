# ============================================================================
# ScholarAssist Dataset Pipeline — Makefile
# ============================================================================
# Usage:
#   make up          — Start the full local dev stack
#   make down        — Stop all containers
#   make logs        — Tail logs from all services
#   make test        — Run the full test suite
#   make lint        — Run linters (ruff + mypy)
#   make seed        — Download sample data and seed OpenSearch
# ============================================================================

.PHONY: help up down logs restart test lint format seed clean

COMPOSE = docker compose

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

# --------------------------------------------------------------------------
# Docker Compose
# --------------------------------------------------------------------------

up: ## Start the full local dev stack
	$(COMPOSE) up -d
	@echo ""
	@echo "🚀 ScholarAssist Dev Stack is running!"
	@echo "   MinIO Console:         http://localhost:9001  (minioadmin/minioadmin)"
	@echo "   OpenSearch:            http://localhost:9200"
	@echo "   OpenSearch Dashboards: http://localhost:5601"
	@echo "   Spark Master UI:       http://localhost:8081"
	@echo "   Airflow UI:            http://localhost:8080  (admin/admin)"
	@echo "   API (Swagger):         http://localhost:8000/docs"
	@echo ""

down: ## Stop all containers and remove orphans
	$(COMPOSE) down --remove-orphans

restart: ## Restart all services
	$(COMPOSE) restart

logs: ## Tail logs from all services
	$(COMPOSE) logs -f

logs-api: ## Tail logs from the API service only
	$(COMPOSE) logs -f api

logs-spark: ## Tail Spark master logs
	$(COMPOSE) logs -f spark-master spark-worker

logs-airflow: ## Tail Airflow logs
	$(COMPOSE) logs -f airflow-webserver airflow-scheduler

# --------------------------------------------------------------------------
# Individual Services
# --------------------------------------------------------------------------

up-api: ## Start only the API and its dependencies (OpenSearch, Redis)
	$(COMPOSE) up -d opensearch redis api

up-spark: ## Start only Spark master and worker
	$(COMPOSE) up -d spark-master spark-worker

up-airflow: ## Start Airflow and its dependencies (Postgres, MinIO)
	$(COMPOSE) up -d postgres minio minio-init airflow-init airflow-webserver airflow-scheduler

# --------------------------------------------------------------------------
# Development
# --------------------------------------------------------------------------

test: ## Run the full test suite
	pytest tests/ -v --tb=short --cov=src --cov-report=term-missing

test-unit: ## Run unit tests only
	pytest tests/unit/ -v --tb=short

test-integration: ## Run integration tests (requires Docker stack)
	pytest tests/integration/ -v --tb=short

lint: ## Run ruff linter and mypy type checker
	ruff check src/ tests/
	mypy src/

format: ## Auto-format code with ruff
	ruff format src/ tests/
	ruff check --fix src/ tests/

# --------------------------------------------------------------------------
# Data Operations
# --------------------------------------------------------------------------

seed: ## Download sample data and seed OpenSearch
	python scripts/download_samples.py
	python scripts/seed_opensearch.py

download-samples: ## Download small sample datasets for development
	python scripts/download_samples.py

# --------------------------------------------------------------------------
# Cleanup
# --------------------------------------------------------------------------

clean: ## Remove all containers, volumes, and cached data
	$(COMPOSE) down -v --remove-orphans
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".mypy_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete 2>/dev/null || true

nuke: ## Full reset — remove everything including volumes
	$(COMPOSE) down -v --rmi local --remove-orphans
	docker volume prune -f
