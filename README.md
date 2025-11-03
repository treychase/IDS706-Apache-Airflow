# MovieLens Airflow ETL Pipeline

## Overview
This repository contains an Apache Airflow pipeline that demonstrates end-to-end data orchestration with parallel processing, database integration, and analysis. The pipeline processes the MovieLens 100K dataset to showcase ETL best practices.

## Pipeline Architecture

### Workflow Steps
1. **Data Ingestion**: Downloads MovieLens 100K dataset (movies and ratings)
2. **Parallel Transformation**: Transforms ratings and movies datasets simultaneously using TaskGroup
3. **Data Merging & Loading**: Combines transformed datasets and loads into PostgreSQL
4. **Analysis**: Reads from database and generates visualization of top-rated movies
5. **Cleanup**: Removes intermediate processed files

### Key Features
- ✅ **Scheduled DAG**: Runs daily (`@daily` schedule)
- ✅ **Parallel Processing**: Transform tasks execute concurrently via TaskGroup
- ✅ **XCom for File Paths**: No actual data passed between tasks, only file paths
- ✅ **PostgreSQL Integration**: Final merged dataset stored in relational database
- ✅ **Data Analysis**: Generates visualization from database query
- ✅ **Cleanup Task**: Removes intermediate files after pipeline completion
- ✅ **Containerized Development**: Includes Dockerfile and devcontainer.json

## Prerequisites
- Docker
- Docker Compose
- 4GB+ available RAM

## Quick Start

### Option 1: Using Make (Recommended)
```bash
# Build Docker images
make build

# Start Airflow and PostgreSQL
make up

# Initialize Airflow (create admin user)
make init

# Trigger the DAG
make trigger

# View logs
make logs

# Stop services
make down

# Clean everything
make clean
```

### Option 2: Manual Docker Compose
```bash
# Start services
docker-compose up --build -d

# Wait for services to initialize (30-60 seconds)
sleep 60

# Create admin user
docker exec -it airflow bash -c "airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com"

# Access Airflow UI
# Navigate to http://localhost:8080
# Login: admin / admin
```

## Accessing the Pipeline

1. **Airflow UI**: http://localhost:8080
   - Username: `admin`
   - Password: `admin`

2. **Enable and Trigger DAG**:
   - Find `movielens_etl_pipeline` in the DAGs list
   - Toggle the DAG to "On"
   - Click the play button to trigger manually

3. **Monitor Execution**:
   - View the Graph view to see task dependencies
   - Check individual task logs for detailed output

## Project Structure

```
.
├── dags/
│   └── movielens_pipeline_dag.py      # Main Airflow DAG definition
├── scripts/
│   ├── download_datasets.py            # Downloads MovieLens data
│   ├── transform_and_merge.py          # Transforms ratings & movies
│   ├── load_to_postgres.py             # Merges and loads to DB
│   └── analysis_read_and_plot.py       # Reads from DB and creates viz
├── sql/
│   └── schema.sql                      # Database schema reference
├── devcontainer/
│   └── devcontainer.json               # VS Code dev container config
├── Dockerfile                          # Custom dev container image
├── docker-compose.yml                  # Orchestrates Airflow + Postgres
├── requirements.txt                    # Python dependencies
├── Makefile                            # Automation commands
└── README.md                           # This file
```

## DAG Task Flow

```
download_task
      ↓
transform_group (parallel execution)
├── transform_ratings_task
└── transform_movies_task
      ↓
merge_and_load_task
      ↓
analysis_task
      ↓
cleanup_task
```

## Database Schema

The pipeline creates and populates the following table in PostgreSQL:

**movie_ratings_merged**
- `userId` (INTEGER)
- `movieId` (INTEGER)
- `rating` (REAL)
- `timestamp` (BIGINT)
- `title` (TEXT)
- `genres` (TEXT)

## Output Files

All outputs are stored in `/opt/airflow/data/` (mounted from `./data/` locally):

- **Raw Data**: `data/ml-100k/` (original dataset)
- **Processed Data**: `data/processed/` (intermediate files - cleaned up after pipeline)
- **Analysis**: `data/analysis/top10_avg_rated_movies.png` (visualization)

## Assignment Requirements Checklist

### 1. Deploy Airflow and Create DAG with Schedule ✅
- DAG scheduled to run `@daily`
- Includes TaskGroup for organizing related tasks
- Start date: November 4, 2025

### 2. Data Ingestion and Transformation ✅
- **Two related datasets**: Movies (u.item) and Ratings (u.data)
- **Transformations applied**: 
  - Ratings: Parse tab-separated values, clean columns
  - Movies: Parse pipe-separated values, extract genres
- **TaskGroup**: `transform_group` contains parallel transform tasks
- **Database**: Final merged data loaded into PostgreSQL

### 3. Analysis ✅
- Reads data from PostgreSQL database
- Analyzes top 10 movies by average rating (minimum 10 ratings)
- Creates bar chart visualization
- Cleanup task removes intermediate processed files

### 4. Documentation ✅
- Comprehensive README explaining pipeline
- Inline code comments in DAG and scripts
- Clear project structure

## Design Principles

### No Data in XCom
Following best practices, tasks communicate via file paths only:
```python
# Download task returns paths
return {'ratings': '/path/to/ratings', 'movies': '/path/to/movies'}

# Transform tasks pull paths and return new paths
ratings_path = ti.xcom_pull(task_ids='download_task')['ratings']
return '/path/to/processed_ratings'
```

### Parallelism
The `transform_group` TaskGroup allows both transform operations to run simultaneously, reducing total pipeline execution time. Configure parallelism in docker-compose:
```yaml
AIRFLOW__CORE__PARALLELISM: "32"
AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG: "16"
```

## Troubleshooting

### Airflow UI Not Loading
```bash
# Check container logs
docker logs airflow

# Restart services
docker-compose restart
```

### Database Connection Issues
```bash
# Verify Postgres is running
docker ps | grep postgres

# Test connection
docker exec -it postgres psql -U airflow -d airflow -c "SELECT 1;"
```

### DAG Not Appearing
```bash
# Check for Python errors in DAG file
docker exec -it airflow airflow dags list

# View DAG parse errors
docker exec -it airflow airflow dags list-import-errors
```

### Permission Issues
```bash
# Fix permissions for mounted volumes
sudo chmod -R 777 logs/ data/
```

## Development Container

This project includes VS Code dev container support:

1. Open folder in VS Code
2. Install "Remote - Containers" extension
3. Click "Reopen in Container" when prompted
4. VS Code will build and attach to the development container

## Screenshots for Submission

Include the following screenshots:
1. **Successful DAG execution** in Airflow UI (Grid view showing all tasks green)
2. **DAG graph view** showing task dependencies and parallel execution
3. **Generated visualization** (`data/analysis/top10_avg_rated_movies.png`)

## Notes

- The pipeline uses the MovieLens 100K dataset (stable, freely available)
- First run may take 2-3 minutes as it downloads and extracts the dataset
- Subsequent runs are faster as the dataset is cached
- The cleanup task only removes intermediate CSV files, not the raw dataset or analysis outputs

## Future Enhancements

- Add data quality checks using Great Expectations
- Implement PySpark for large-scale transformations (Super Bonus option)
- Add monitoring and alerting
- Parameterize dataset size (100K, 1M, 10M, etc.)
- Implement incremental loading strategy

## License

This is an educational project for Duke University coursework.