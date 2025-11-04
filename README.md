# OpenFlights ETL Pipeline with Apache Airflow

# Screenshots

## DAG Pass
![E850729D-D501-4A9A-BB00-278E43B0CC69_1_105_c](https://github.com/user-attachments/assets/012739d5-20cb-479e-9990-645434bc32b6)

![3598C0DC-8E6A-4A13-A445-52808C927F1A_1_105_c](https://github.com/user-attachments/assets/0b92b12f-9167-4c5a-8ef4-b084099831f1)

![20674A0A-39E8-49F1-A3ED-9830BAD835D2_4_5005_c](https://github.com/user-attachments/assets/db1b6375-edc0-4c46-ace2-24d604c1ad58)

## Analysis

![FFE835F6-B950-4857-8618-119FE0812D79_1_105_c](https://github.com/user-attachments/assets/faa8bac6-e666-4ff5-9835-d4836d626153)

## 🎯 Project Overview

This pipeline processes over 67,000 flight routes and 7,600 airports worldwide, performing ETL operations and generating comprehensive aviation analytics. The system is fully containerized using Docker and orchestrated with Apache Airflow.

## 📊 Analysis Results

The pipeline generates four key visualizations showing global aviation patterns:

### Top 15 Airlines by Route Count
Leading carriers by number of routes operated:
- **Ryanair (FR)**: 2,484 routes
- **American Airlines (AA)**: 2,449 routes  
- **United Airlines (UA)**: 2,323 routes
- **Lufthansa (LH)**: 2,155 routes
- **Air France (AF)**: 1,851 routes

### Top 15 Countries by Outgoing Routes
Global distribution of flight connectivity:
- **United States**: 13,000+ routes (dominant aviation hub)
- **China**: 8,500+ routes
- **United Kingdom**: 2,900+ routes
- **Spain**: 2,800+ routes
- **Germany**: 2,600+ routes

### Route Distance Analysis
- **Mean Distance**: 1,856 km
- **Median Distance**: 1,199 km
- **Distribution**: Most routes are short-to-medium haul (under 3,000 km)
- **Peak**: Majority of routes fall in the 500-2,000 km range

### Top 15 Busiest Airports
Major global aviation hubs by outgoing routes:
- **Hartsfield-Jackson Atlanta (ATL)**: 915 routes
- **Chicago O'Hare (ORD)**: 854 routes
- **Beijing Capital (PEK)**: 792 routes
- **London Heathrow (LHR)**: 758 routes
- **Charles de Gaulle Paris (CDG)**: 732 routes

![OpenFlights Analysis Results](screenshots/analysis_results.png)
*Four-panel visualization showing airline rankings, country connectivity, distance distribution, and busiest airports*

## 🏗️ Architecture

```
┌─────────────────┐
│  Data Sources   │
│  (OpenFlights)  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Download      │ ← Task 1: Fetch datasets
│   (Python)      │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Transform     │ ← Task 2: Clean & process
│   (Pandas)      │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Load to DB     │ ← Task 3: Merge & store
│  (PostgreSQL)   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Analysis      │ ← Task 4: Generate insights
│  (Matplotlib)   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Cleanup       │ ← Task 5: Remove temp files
└─────────────────┘
```

## 🚀 Quick Start

### Prerequisites
- Docker & Docker Compose
- Git
- Make (optional, for convenience commands)

### Installation

1. **Clone the repository**
```bash
git clone https://github.com/YOUR_USERNAME/YOUR_REPO.git
cd YOUR_REPO
```

2. **Build and start services**
```bash
# Using Make (recommended)
make build
make up
make init

# Or using Docker Compose directly
docker-compose build
docker-compose up -d
docker-compose exec airflow-webserver airflow db init
```

3. **Access Airflow UI**
```
URL: http://localhost:8080
Username: admin
Password: admin
```

4. **Run the pipeline**
- Navigate to DAGs page
- Enable `openflights_etl_pipeline`
- Click "Trigger DAG"

### Project Structure
```
.
├── dags/
│   └── openflights_pipeline_dag.py    # Airflow DAG definition
├── scripts/
│   ├── download_datasets.py           # Fetch OpenFlights data
│   ├── transform_and_merge.py         # Data cleaning
│   ├── load_to_postgres.py            # Database operations
│   └── analysis_read_and_plot.py      # Generate visualizations
├── tests/
│   ├── test_download.py               # Unit tests
│   ├── test_transform.py
│   ├── test_load.py
│   └── test_analysis.py
├── docker-compose.yml                 # Container orchestration
├── Dockerfile.airflow                 # Airflow custom image
├── requirements.txt                   # Python dependencies
├── Makefile                           # Convenience commands
└── README.md
```

## 📋 Pipeline Tasks

### Task 1: Download Data
- **Source**: OpenFlights GitHub repository
- **Files**: `airports.dat` (7,698 airports), `routes.dat` (67,663 routes)
- **Format**: CSV with predefined schemas

### Task 2: Transform Data
- **Airports**: Parse 14 columns (ID, name, city, country, IATA, ICAO, lat, lon, etc.)
- **Routes**: Parse 9 columns (airline, source, destination, stops, equipment)
- **Validation**: Remove invalid entries, check data types
- **Output**: Cleaned CSV files

### Task 3: Load to Database
- **Operation**: Inner join routes with airports (twice for source + destination)
- **Table**: `airport_routes_merged` with 22 columns
- **Rows**: ~66,771 valid routes (99.4% retention)
- **Schema**: Route info + source airport details + destination airport details

### Task 4: Analysis & Visualization
- **Calculations**: Haversine distance for all routes
- **Charts**: 4-panel matplotlib figure (airlines, countries, distances, airports)
- **Statistics**: Mean/median distances, route counts, rankings
- **Output**: `airport_analysis.png` (1200x900, 150 DPI)

### Task 5: Cleanup
- **Action**: Remove intermediate CSV files
- **Trigger**: Always runs (even if upstream fails)

## 🛠️ Development

### Running Tests
```bash
# Run all tests
pytest tests/ -v

# Run specific test file
pytest tests/test_transform.py -v

# Run with coverage
pytest --cov=scripts tests/
```

### Makefile Commands
```bash
make build       # Build Docker images
make up          # Start containers
make down        # Stop containers
make logs        # View container logs
make test        # Run test suite
make clean       # Remove data and temp files
make reset       # Complete reset (down + clean + build + up)
```

### Adding New Analysis
1. Modify `scripts/analysis_read_and_plot.py`
2. Update SQL queries if needed
3. Rebuild container: `make build`
4. Restart services: `make restart`

## 🔍 Data Quality Metrics

| Metric | Value |
|--------|-------|
| Input Routes | 67,663 |
| Valid Routes (after filtering) | 66,771 (98.7%) |
| Unique Airlines | 600+ |
| Unique Airports | 3,199 |
| Countries Covered | 225 |
| Direct Flights | 99.98% |
| Routes with Distance Calculations | 66,771 (100%) |

## 📈 Key Insights

1. **US Dominance**: The United States has nearly 50% more routes than the second-place country (China)
2. **Short-Haul Focus**: 60% of all routes are under 1,500 km, indicating strong regional connectivity
3. **Hub Concentration**: Top 15 airports handle over 10,000 routes combined
4. **Low-Cost Carriers**: Budget airlines like Ryanair lead in route count
5. **Direct Connections**: Nearly all routes are direct flights (minimal multi-stop itineraries)

## 🐛 Troubleshooting

### Issue: DAG not appearing in Airflow
**Solution**: Check DAG file for syntax errors
```bash
docker-compose exec airflow-webserver airflow dags list
docker-compose logs airflow-scheduler | grep -i error
```

### Issue: Database connection failed
**Solution**: Verify PostgreSQL container is running
```bash
docker-compose ps
docker-compose exec postgres psql -U airflow -d airflow -c "SELECT 1;"
```

### Issue: Analysis chart not generating
**Solution**: Check write permissions and dependencies
```bash
docker-compose exec airflow-webserver ls -la /opt/airflow/data/analysis/
docker-compose exec airflow-webserver pip list | grep matplotlib
```

## 📚 Technologies Used

- **Orchestration**: Apache Airflow 2.7.1
- **Database**: PostgreSQL 13
- **Container**: Docker & Docker Compose
- **Languages**: Python 3.11
- **Libraries**: 
  - pandas (data processing)
  - SQLAlchemy (database ORM)
  - matplotlib & seaborn (visualization)
  - requests (HTTP client)
  - pytest (testing)

## 📄 License

This project is licensed under the MIT License. Data sourced from [OpenFlights](https://openflights.org/data.html) under the Open Database License.

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/new-analysis`)
3. Commit changes (`git commit -am 'Add route profitability analysis'`)
4. Push to branch (`git push origin feature/new-analysis`)
5. Open a Pull Request

## 📞 Contact

For questions or feedback, please open an issue on GitHub.

---

