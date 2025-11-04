# Makefile for OpenFlights Airflow ETL pipeline
# Usage:
#   make build       → Build Docker images
#   make up          → Start Airflow & Postgres
#   make init        → Initialize Airflow database and user
#   make trigger     → Trigger the OpenFlights DAG
#   make down        → Stop containers
#   make logs        → View Airflow logs
#   make lint        → Lint Python code
#   make clean       → Remove all containers, images, volumes
#   make status      → Check service status
#   make restart     → Restart services

PROJECT_NAME = openflights_airflow
AIRFLOW_DAG = openflights_etl_pipeline
AIRFLOW_CONTAINER = airflow
POSTGRES_CONTAINER = postgres

.PHONY: build up down clean init trigger lint logs status restart db-connect help troubleshoot setup-dirs

# ------------------------------------------------------------------------------
# Main commands
# ------------------------------------------------------------------------------

setup-dirs:
	@echo "📁 Creating required directories..."
	@mkdir -p logs data data/analysis data/processed
	@chmod 777 logs data
	@echo "✅ Directories created!"

build: setup-dirs
	@echo "🔧 Building Docker images..."
	docker-compose build
	@echo "✅ Build complete!"

up: setup-dirs
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
	@echo "🔍 Querying airport_routes_merged table..."
	@docker exec -it $(POSTGRES_CONTAINER) psql -U airflow -d airflow -c "SELECT COUNT(*) as total_rows FROM airport_routes_merged;" 2>/dev/null || echo "❌ Table not created yet. Run the DAG first."

list-dags:
	@echo "📋 Available DAGs:"
	@docker exec -it $(AIRFLOW_CONTAINER) airflow dags list

dag-status:
	@echo "📊 DAG Status: $(AIRFLOW_DAG)"
	@docker exec -it $(AIRFLOW_CONTAINER) airflow dags list | grep $(AIRFLOW_DAG) || echo "❌ DAG not found"

get-plot:
	@echo "📊 Copying visualization from container..."
	@docker cp $(AIRFLOW_CONTAINER):/opt/airflow/data/analysis/airport_analysis.png ./airport_analysis.png 2>/dev/null && echo "✅ Saved to: ./airport_analysis.png" || echo "❌ Plot not found. Run the DAG first with 'make trigger'"

troubleshoot:
	@echo "🔍 Running troubleshooter..."
	@echo ""
	@echo "1. Checking containers..."
	@docker ps | grep -E "(airflow|postgres)" || echo "❌ Containers not running!"
	@echo ""
	@echo "2. Checking data files..."
	@docker exec -it $(AIRFLOW_CONTAINER) ls -lh /opt/airflow/data/ 2>/dev/null || echo "❌ No data directory"
	@echo ""
	@echo "3. Checking database..."
	@docker exec -it $(POSTGRES_CONTAINER) psql -U airflow -d airflow -c "SELECT COUNT(*) FROM airport_routes_merged;" 2>/dev/null || echo "❌ Table doesn't exist"
	@echo ""
	@echo "4. Checking DAG runs..."
	@docker exec -it $(AIRFLOW_CONTAINER) airflow dags list-runs -d $(AIRFLOW_DAG) 2>/dev/null | head -5 || echo "❌ No runs"
	@echo ""
	@echo "📝 Check Airflow UI: http://localhost:8080"

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

clean-logs:
	@echo "🗑️  Removing logs directory..."
	rm -rf logs/
	@echo "✅ Logs cleaned!"

clean-all: clean clean-data clean-logs
	@echo "🧹 Full cleanup complete!"

# ------------------------------------------------------------------------------
# Help
# ------------------------------------------------------------------------------

help:
	@echo "📖 OpenFlights Airflow ETL Pipeline - Available Commands"
	@echo ""
	@echo "Setup & Start:"
	@echo "  make setup-dirs   - Create required directories"
	@echo "  make build        - Build Docker images"
	@echo "  make up           - Start services (Airflow + PostgreSQL)"
	@echo "  make init         - Initialize Airflow DB and create admin user"
	@echo "  make down         - Stop all services"
	@echo "  make restart      - Restart all services"
	@echo ""
	@echo "Pipeline Operations:"
	@echo "  make trigger      - Trigger the OpenFlights DAG"
	@echo "  make list-dags    - List all available DAGs"
	@echo "  make dag-status   - Check OpenFlights DAG status"
	@echo "  make get-plot     - Copy visualization from container"
	@echo ""
	@echo "Monitoring:"
	@echo "  make status       - Show service status"
	@echo "  make logs         - Stream Airflow logs"
	@echo "  make logs-all     - Stream all logs"
	@echo "  make troubleshoot - Run diagnostic checks"
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
	@echo "  make clean-logs   - Remove logs directory"
	@echo "  make clean-all    - Full cleanup"
	@echo ""
	@echo "Quick Start:"
	@echo "  1. make build"
	@echo "  2. make up"
	@echo "  3. make init"
	@echo "  4. make trigger"
	@echo "  5. make get-plot"
	@echo "  6. Visit http://localhost:8080 (admin/admin)"

.DEFAULT_GOAL := help
