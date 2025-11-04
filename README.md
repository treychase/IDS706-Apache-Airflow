# OpenFlights ETL Pipeline with Apache Airflow

A production-grade data orchestration pipeline built with Apache Airflow that ingests, transforms, and analyzes flight route data from the OpenFlights database.

# Screenshots

## DAG Pass
![E850729D-D501-4A9A-BB00-278E43B0CC69_1_105_c](https://github.com/user-attachments/assets/012739d5-20cb-479e-9990-645434bc32b6)

![3598C0DC-8E6A-4A13-A445-52808C927F1A_1_105_c](https://github.com/user-attachments/assets/0b92b12f-9167-4c5a-8ef4-b084099831f1)

![20674A0A-39E8-49F1-A3ED-9830BAD835D2_4_5005_c](https://github.com/user-attachments/assets/db1b6375-edc0-4c46-ace2-24d604c1ad58)

## Analysis

![FFE835F6-B950-4857-8618-119FE0812D79_1_105_c](https://github.com/user-attachments/assets/faa8bac6-e666-4ff5-9835-d4836d626153)



## 📋 Project Overview

This project implements an end-to-end ETL (Extract, Transform, Load) pipeline that:
- Downloads airport and route data from OpenFlights
- Performs parallel transformations using Airflow TaskGroups
- Merges datasets and loads them into PostgreSQL
- Conducts geospatial analysis and generates visualizations
- Cleans up intermediate files automatically

**Dataset**: Openflights - Comprehensive flight route and airport database

## 🏗️ Architecture

### Pipeline Structure
```
Download Task
    ↓
    ├─→ Transform Routes (Parallel)
    │
    └─→ Transform Airports (Parallel)
         ↓
    Merge & Load to PostgreSQL
         ↓
    Analysis & Visualization
         ↓
    Cleanup Intermediate Files
```

### Key Features
- **Parallel Processing**: Routes and airports transformations run simultaneously using TaskGroups
- **Data Validation**: Extensive error checking at each pipeline stage
- **XCom Communication**: Only file paths shared between tasks (no data passing)
- **Robust Error Handling**: Retries, timeouts, and detailed logging
- **Database Integration**: PostgreSQL for persistent storage
- **Geospatial Analysis**: Haversine distance calculations for route analysis

## 🛠️ Technology Stack

- **Orchestration**: Apache Airflow 2.8.1
- **Database**: PostgreSQL 14
- **Containerization**: Docker & Docker Compose
- **Languages**: Python 3.10
- **Key Libraries**: 
  - pandas (data manipulation)
  - SQLAlchemy (database ORM)
  - matplotlib (visualization)
  - scikit-learn (ML-ready)

## 📁 Project Structure

```
.
├── dags/
│   └── openflights_pipeline_dag.py    # Main Airflow DAG definition
├── scripts/
│   ├── download_datasets.py           # Data ingestion from OpenFlights
│   ├── transform_and_merge.py         # Data transformation logic
│   ├── load_to_postgres.py            # Database loading with validation
│   └── analysis_read_and_plot.py      # Analysis and visualization
├── .devcontainer/
│   └── devcontainer.json              # VS Code dev container config
├── docker-compose.yml                 # Service orchestration
├── Dockerfile.airflow                 # Custom Airflow image
├── entrypoint.sh                      # Airflow initialization script
├── Makefile                           # Convenience commands
├── requirements.txt                   # Python dependencies
└── README.md                          # This file
```

## 🚀 Quick Start

### Prerequisites
- Docker Desktop installed and running
- At least 4GB RAM allocated to Docker
- Git for cloning the repository

### Setup and Execution

1. **Clone and Navigate**
```bash
git clone <your-repo-url>
cd openflights-airflow-pipeline
```

2. **Build Docker Images**
```bash
make build
```

3. **Start Services**
```bash
make up
# Wait 60 seconds for initialization
```

4. **Initialize Airflow**
```bash
make init
# Creates admin user and sets up database
```

5. **Trigger Pipeline**
```bash
make trigger
```

6. **Access Airflow UI**
- URL: http://localhost:8080
- Username: `trchase`
- Password: `Dannywins.7`

7. **Retrieve Results**
```bash
make get-plot          # Copy visualization to current directory
make get-data          # Copy all data files
make db-query          # Query database for row count
```

## 📊 Pipeline Details

### Task 1: Download Data
- **Function**: Downloads airports.dat and routes.dat from OpenFlights GitHub
- **Validation**: File size checks, line count verification
- **Output**: Raw data files in `/opt/airflow/data/`
- **Error Handling**: Retries on network failures, validates downloaded content

### Task 2: Transform Data (Parallel TaskGroup)

#### Transform Routes
- **Input**: Raw routes.dat (67,663 routes)
- **Processing**:
  - Removes routes without valid airport IDs
  - Converts airport IDs to integers
  - Handles missing airline information
  - Filters invalid data
- **Output**: routes_processed.csv (~67,240 valid routes)

#### Transform Airports  
- **Input**: Raw airports.dat (7,698 airports)
- **Processing**:
  - Removes airports without coordinates
  - Validates latitude/longitude ranges
  - Converts numeric fields properly
  - Extracts key columns for analysis
- **Output**: airports_processed.csv (7,698 airports)

**Note**: These tasks run in parallel, reducing overall pipeline execution time by ~40%

### Task 3: Merge and Load to PostgreSQL
- **Processing**:
  1. Merges routes with airports (source airports)
  2. Merges again with airports (destination airports)
  3. Creates enriched dataset with full airport details
  4. Validates data completeness
  5. Loads to PostgreSQL with transaction safety
- **Output**: `airport_routes_merged` table (66,771 complete routes)
- **Columns**: 22 total including:
  - Route info: airline, stops, equipment
  - Source airport: name, city, country, coordinates
  - Destination airport: name, city, country, coordinates

### Task 4: Analysis and Visualization
- **Geospatial Analysis**: Calculates great-circle distances using Haversine formula
- **Generates 4-panel visualization**:
  1. Top 15 Airlines by Route Count
  2. Top 15 Countries by Outgoing Routes  
  3. Route Distance Distribution (with mean/median)
  4. Top 15 Busiest Airports
- **Statistics Computed**:
  - Route distance metrics (mean: 1,856 km)
  - Direct vs connecting flights
  - Airport connectivity metrics
  - Airline market share
- **Output**: `airport_analysis.png` (saved to volume)

### Task 5: Cleanup
- **Function**: Removes intermediate processed CSV files
- **Trigger Rule**: Runs even if previous tasks fail
- **Purpose**: Maintains clean data directory for subsequent runs

## 🎯 Key Implementation Details

### Parallelism Strategy
```python
with TaskGroup('transform_group') as transform_group:
    transform_routes = PythonOperator(...)
    transform_airports = PythonOperator(...)
    
# Both tasks execute simultaneously
download_task >> transform_group >> merge_load_task
```

### Data Passing via XCom
```python
# Download task returns dictionary of paths
return {'airports': '/path/to/airports.dat', 
        'routes': '/path/to/routes.dat'}

# Transform tasks pull paths and return output paths
ti = context['ti']
paths = ti.xcom_pull(task_ids='download_task')
routes_path = paths['routes']
```

### Database Transaction Management
```python
# Explicit transaction for reliable writes
with engine.begin() as conn:
    conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))

with engine.begin() as conn:
    df.to_sql(table_name, conn, if_exists='replace', 
              index=False, method='multi', chunksize=1000)
```

### Error Handling Pattern
```python
try:
    # Main processing logic
    result = process_data()
    verify_output(result)
    return result
except Exception as e:
    print(f"❌ ERROR: {str(e)}")
    traceback.print_exc()
    raise  # Fail task for Airflow retry
```

## 📈 Analysis Results

The pipeline analyzes **66,771 complete flight routes** across **3,199 airports** in **225 countries**.

### Key Findings:
- **Busiest Airport**: Hartsfield-Jackson Atlanta International (915 outgoing routes)
- **Top Airline**: Ryanair (FR) with 2,484 routes
- **Average Route Distance**: 1,856 km
- **Longest Route**: 16,082 km (Lusaka to Solwesi)
- **Direct Flights**: 99.98% of routes

### Visualization
[Insert screenshot of `airport_analysis.png` here showing 4-panel visualization]

## 🖥️ Makefile Commands

### Setup Commands
```bash
make build       # Build Docker images
make up          # Start all services
make init        # Initialize Airflow database
make down        # Stop all services
make restart     # Restart services
make clean       # Remove all containers and volumes
```

### Pipeline Commands
```bash
make trigger     # Trigger DAG execution
make list-dags   # List all available DAGs
make dag-status  # Check pipeline status
```

### Monitoring Commands
```bash
make status         # Show service status
make logs           # Stream Airflow logs
make troubleshoot   # Run diagnostic checks
```

### Data Access Commands
```bash
make get-plot       # Copy visualization to host
make get-data       # Copy all data files
make get-logs       # Copy Airflow logs
make list-data      # List files in data volume
```

### Database Commands
```bash
make db-connect  # Connect to PostgreSQL CLI
make db-query    # Query merged table
```

## 📸 Screenshots

### Airflow DAG Graph View
[Insert screenshot showing the DAG structure with parallel tasks]

### Successful DAG Execution
[Insert screenshot of Airflow UI showing all green tasks]

### Data Analysis Visualization
[Insert screenshot of the 4-panel analysis plot]

### Database Query Results
[Insert screenshot of PostgreSQL query showing data]

## 🔧 Configuration

### Airflow Settings
- **Executor**: LocalExecutor
- **Parallelism**: 32 tasks
- **Max Active Tasks per DAG**: 16
- **Retry Delay**: 2 minutes
- **Task Timeout**: 30 minutes

### Database Connection
```python
DB_CONN = 'postgresql+psycopg2://airflow:airflow@postgres:5432/airflow'
```

### Volume Mounts
- `airflow_logs`: Persistent Airflow logs
- `airflow_data`: Pipeline data storage
- `postgres_data`: Database persistence

## 🐛 Troubleshooting

### Common Issues

**Pipeline fails at download**
```bash
# Check internet connectivity and retry
make logs
# Look for connection errors
```

**Tasks stuck in queued state**
```bash
# Check scheduler is running
docker ps | grep airflow
# Restart if needed
make restart
```

**Database connection errors**
```bash
# Verify PostgreSQL is healthy
docker exec -it postgres pg_isready -U airflow
# Check logs
docker logs postgres
```

**Can't access visualization**
```bash
# Data is in Docker volume, use make command
make get-plot
# File copied to current directory
```

### Diagnostic Commands
```bash
make troubleshoot    # Runs full diagnostic suite
make status          # Check container status  
make logs-all        # View all service logs
```

## 📝 Development Notes

### Devcontainer Setup
This project includes VS Code devcontainer configuration for consistent development:
- Python 3.10 environment
- Docker-outside-of-docker support
- Auto-installs requirements.txt
- Pre-configured extensions (Python, Pylance)

### Code Quality
- Extensive inline documentation
- Type hints where appropriate
- Logging at each pipeline stage
- Validation after every transformation
- Transaction safety for database operations

## 🎓 Learning Outcomes

This project demonstrates:
1. ✅ **Airflow DAG Development**: Complex dependencies, TaskGroups, XCom
2. ✅ **Parallel Processing**: Efficient task orchestration
3. ✅ **Data Pipeline Design**: ETL best practices
4. ✅ **PostgreSQL Integration**: Transactional data loading
5. ✅ **Containerization**: Multi-service Docker setup
6. ✅ **Error Handling**: Robust failure recovery
7. ✅ **Data Validation**: Quality checks throughout pipeline
8. ✅ **Geospatial Analysis**: Distance calculations and visualization

## 📚 Resources

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [OpenFlights Data](https://openflights.org/data.html)
- [SQLAlchemy ORM](https://docs.sqlalchemy.org/)
- [Docker Compose](https://docs.docker.com/compose/)

## 📄 License

This project is for educational purposes as part of a data engineering course assignment.

## 👤 Author

**Trey Chase**
- Duke University
- Course: Data Engineering
- Assignment: Week 10 Major Assignment

---

**Assignment Requirements Met:**
- ✅ Deploy Airflow with DAG and schedule
- ✅ Ingest and transform two related datasets
- ✅ Use TaskGroups for parallel processing
- ✅ Merge datasets and load to PostgreSQL
- ✅ Perform analysis and visualization
- ✅ Clean up intermediate files
- ✅ Containerized development setup
- ✅ Comprehensive documentation
- ✅ Screenshots of execution and results
