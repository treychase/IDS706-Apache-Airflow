# Makefile for MovieLens Airflow ETL pipeline
# Usage:
#   make build       → Build Docker images
#   make up          → Start Airflow & Postgres
#   make init        → Initialize Airflow database and user
#   make trigger     → Trigger the MovieLens DAG
#   make down        → Stop containers
#   make logs        → View Airflow logs
#   make lint        → Lint Python code
#   make clean       → Remove all containers, images, volumes
#   make status      → Check service status
#   make restart     → Restart services

PROJECT_NAME = movielens_airflow
AIRFLOW_DAG = movielens_etl_pipeline
AIRFLOW_CONTAINER = airflow
POSTGRES_CONTAINER = postgres

.PHONY: build up down clean init trigger lint logs status restart db-connect help

# ------------------------------------------------------------------------------
# Main commands
# ------------------------------------------------------------------------------

build:
	@echo "🔧 Building Docker images..."
	docker-compose build
	@echo "✅ Build complete!"

up:
	@echo "🚀 Starting Airflow and Postgres..."
	docker-compose up -d
	@echo "⏳ Waiting for services to initialize (60 seconds)..."
	@sleep 60
	@echo "✅ Services started!"
	@echo "📊 Airflow UI: http://localhost:8080 (admin/admin)"
	@make status

init:
	@echo "🧩 Initializing Airflow database and creating admin user..."
	@docker exec -it $(AIRFLOW_CONTAINER) bash -c "airflow db upgrade" || true
	@docker exec -it $(AIRFLOW_CONTAINER) bash -c "airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com" 2>/dev/null || echo "ℹ️  Admin user already exists"
	@echo "✅ Initialization complete!"
	@echo "📊 Login to http://localhost:8080 with admin/admin"

trigger:
	@echo "▶️  Triggering DAG: $(AIRFLOW_DAG)"
	@docker exec -it $(AIRFLOW_CONTAINER) bash -c "airflow dags unpause $(AIRFLOW_DAG)" || true
	@docker exec -it $(AIRFLOW_CONTAINER) bash -c "airflow dags trigger $(AIRFLOW_DAG)"
	@echo "✅ DAG triggered! Check http://localhost:8080 for execution status"

down:
	@echo "🛑 Stopping containers..."
	docker-compose down
	@echo "✅ Services stopped!"

restart:
	@echo "🔄 Restarting services..."
	@make down
	@make up
	@echo "✅ Services restarted!"

# ------------------------------------------------------------------------------
# Utility commands
# ------------------------------------------------------------------------------

status:
	@echo "📊 Service Status:"
	@docker-compose ps

logs:
	@echo "📋 Streaming Airflow logs (Ctrl+C to exit)..."
	docker-compose logs -f $(AIRFLOW_CONTAINER)

logs-all:
	@echo "📋 Streaming all logs (Ctrl+C to exit)..."
	docker-compose logs -f

db-connect:
	@echo "🔌 Connecting to PostgreSQL..."
	docker exec -it $(POSTGRES_CONTAINER) psql -U airflow -d airflow

db-query:
	@echo "🔍 Querying movie_ratings_merged table..."
	@docker exec -it $(POSTGRES_CONTAINER) psql -U airflow -d airflow -c "SELECT COUNT(*) as total_rows FROM movie_ratings_merged;" 2>/dev/null || echo "❌ Table not created yet. Run the DAG first."

list-dags:
	@echo "📋 Available DAGs:"
	@docker exec -it $(AIRFLOW_CONTAINER) airflow dags list

dag-status:
	@echo "📊 DAG Status: $(AIRFLOW_DAG)"
	@docker exec -it $(AIRFLOW_CONTAINER) airflow dags list | grep $(AIRFLOW_DAG) || echo "❌ DAG not found"

lint:
	@echo "🧪 Running code linting..."
	@docker run --rm -v "$$PWD:/workspace" -w /workspace python:3.10 bash -c " \
		pip install -q black flake8 && \
		echo '▶️  Running black...' && \
		black --check dags scripts 2>/dev/null || (black dags scripts && echo '✅ Code formatted') && \
		echo '▶️  Running flake8...' && \
		flake8 dags scripts --max-line-length 100 --ignore=E203,W503 --exclude=__pycache__ \
	"
	@echo "✅ Linting complete!"

format:
	@echo "🎨 Formatting code with black..."
	@docker run --rm -v "$$PWD:/workspace" -w /workspace python:3.10 bash -c " \
		pip install -q black && \
		black dags scripts \
	"
	@echo "✅ Code formatted!"

# ------------------------------------------------------------------------------
# Cleanup commands
# ------------------------------------------------------------------------------

clean:
	@echo "🧹 Cleaning up all resources..."
	docker-compose down -v --remove-orphans
	@echo "🗑️  Removing unused Docker resources..."
	docker system prune -f
	@echo "✅ Cleanup complete!"

clean-data:
	@echo "🗑️  Removing data directory..."
	rm -rf data/
	@echo "✅ Data cleaned!"

clean-all: clean clean-data
	@echo "🧹 Full cleanup complete!"

# ------------------------------------------------------------------------------
# Help
# ------------------------------------------------------------------------------

help:
	@echo "📖 MovieLens Airflow ETL Pipeline - Available Commands"
	@echo ""
	@echo "Setup & Start:"
	@echo "  make build        - Build Docker images"
	@echo "  make up           - Start services (Airflow + PostgreSQL)"
	@echo "  make init         - Initialize Airflow DB and create admin user"
	@echo "  make down         - Stop all services"
	@echo "  make restart      - Restart all services"
	@echo ""
	@echo "Pipeline Operations:"
	@echo "  make trigger      - Trigger the MovieLens DAG"
	@echo "  make list-dags    - List all available DAGs"
	@echo "  make dag-status   - Check MovieLens DAG status"
	@echo ""
	@echo "Monitoring:"
	@echo "  make status       - Show service status"
	@echo "  make logs         - Stream Airflow logs"
	@echo "  make logs-all     - Stream all logs"
	@echo ""
	@echo "Database:"
	@echo "  make db-connect   - Connect to PostgreSQL CLI"
	@echo "  make db-query     - Query the merged table"
	@echo ""
	@echo "Development:"
	@echo "  make lint         - Check code quality"
	@echo "  make format       - Format code with black"
	@echo ""
	@echo "Cleanup:"
	@echo "  make clean        - Remove containers and volumes"
	@echo "  make clean-data   - Remove data directory"
	@echo "  make clean-all    - Full cleanup"
	@echo ""
	@echo "Quick Start:"
	@echo "  1. make build"
	@echo "  2. make up"
	@echo "  3. make init"
	@echo "  4. make trigger"
	@echo "  5. Visit http://localhost:8080 (admin/admin)"

.DEFAULT_GOAL := help