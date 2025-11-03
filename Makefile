# Makefile for MovieLens Airflow ETL pipeline
# Usage:
#   make build       → Build Docker images
#   make up          → Start Airflow & Postgres
#   make init        → Initialize Airflow database and user
#   make trigger     → Trigger the MovieLens DAG
#   make down        → Stop containers
#   make lint        → Lint Python code
#   make clean       → Remove all containers, images, volumes

PROJECT_NAME = movielens_airflow
AIRFLOW_DAG = movielens_etl_pipeline
AIRFLOW_CONTAINER = airflow
POSTGRES_CONTAINER = postgres

.PHONY: build up down clean init trigger lint logs

# ------------------------------------------------------------------------------

build:
	@echo "🔧 Building Docker images..."
	docker-compose build

up:
	@echo "🚀 Starting Airflow and Postgres..."
	docker-compose up -d
	@echo "Waiting for Airflow to initialize..."
	sleep 10
	docker ps

init:
	@echo "🧩 Initializing Airflow database and user..."
	docker exec -it $(AIRFLOW_CONTAINER) bash -c "airflow db upgrade && airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com || true"

trigger:
	@echo "▶️ Triggering DAG: $(AIRFLOW_DAG)"
	docker exec -it $(AIRFLOW_CONTAINER) bash -c "airflow dags trigger $(AIRFLOW_DAG)"

down:
	@echo "🧹 Stopping and removing containers..."
	docker-compose down

logs:
	docker-compose logs -f $(AIRFLOW_CONTAINER)

lint:
	@echo "🧪 Running code linting..."
	docker run --rm -v $$PWD:/workspace -w /workspace python:3.10 bash -c "\
		pip install black flake8 && \
		echo 'Running black...' && black --check dags scripts && \
		echo 'Running flake8...' && flake8 dags scripts --max-line-length 100 --ignore=E203,W503 \
	"

clean:
	@echo "🧽 Cleaning up all containers, images, and volumes..."
	docker-compose down -v --remove-orphans
	docker system prune -f
