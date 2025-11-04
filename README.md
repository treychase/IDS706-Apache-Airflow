# MovieLens Pipeline Fix Documentation

## Problem Diagnosis

The pipeline was not creating the `movie_ratings_merged` table in PostgreSQL. This could be due to:

1. **Transaction not being committed properly** - SQLAlchemy transactions need explicit handling
2. **Silent failures** - Insufficient error logging made it hard to diagnose
3. **Data validation issues** - Missing checks for file existence and data integrity

## Solution: Improved Scripts

I've created **3 improved scripts** with better error handling, verbose logging, and explicit transaction management:

### 1. `transform_and_merge_fixed.py`
**Improvements:**
- ✅ File existence validation
- ✅ Detailed data quality checks
- ✅ Sample data display for debugging
- ✅ Output file verification
- ✅ Genre statistics and validation
- ✅ Standalone test mode

### 2. `load_to_postgres_fixed.py`
**Improvements:**
- ✅ Explicit DROP TABLE before write (ensures clean state)
- ✅ Separate transactions for write and verify
- ✅ Row count verification after write
- ✅ Column structure validation
- ✅ Sample data retrieval to confirm write
- ✅ Better error messages

### 3. `test_pipeline.py`
**Improvements:**
- ✅ End-to-end pipeline test outside Airflow
- ✅ Step-by-step execution with detailed logging
- ✅ Database verification with statistics
- ✅ Helps identify exactly where failures occur

## How to Apply the Fixes

### Option 1: Replace Files in Production (Recommended)

```bash
# Copy fixed files to the scripts directory
cp /home/claude/transform_and_merge_fixed.py /mnt/user-data/outputs/transform_and_merge.py
cp /home/claude/load_to_postgres_fixed.py /mnt/user-data/outputs/load_to_postgres.py
cp /home/claude/test_pipeline.py /mnt/user-data/outputs/test_pipeline.py

# Then manually copy these to your scripts/ directory on the host machine
# and rebuild the containers
```

### Option 2: Test First, Then Deploy

```bash
# 1. Copy test script to outputs
cp /home/claude/test_pipeline.py /mnt/user-data/outputs/

# 2. On your host machine, copy it to the project
cp data/outputs/test_pipeline.py scripts/

# 3. Run the test inside the Airflow container
docker exec -it airflow python /opt/airflow/scripts/test_pipeline.py

# 4. If successful, replace the original scripts
cp /home/claude/transform_and_merge_fixed.py /mnt/user-data/outputs/transform_and_merge.py
cp /home/claude/load_to_postgres_fixed.py /mnt/user-data/outputs/load_to_postgres.py

# 5. Copy to your host scripts/ directory
# 6. Rebuild and restart
make down
make build
make up
make trigger
```

## Quick Diagnostic Commands

### Check if files were created:
```bash
docker exec -it airflow ls -lh /opt/airflow/data/processed/
```

### Check database directly:
```bash
docker exec -it postgres psql -U airflow -d airflow -c "SELECT COUNT(*) FROM movie_ratings_merged;"
```

### Run test pipeline:
```bash
docker exec -it airflow python /opt/airflow/scripts/test_pipeline.py
```

### Check Airflow task logs:
```bash
docker exec -it airflow airflow tasks test movielens_etl_pipeline merge_and_load_task 2025-11-04
```

## Key Differences from Original

### Original `load_to_postgres.py`:
```python
with engine.begin() as conn:
    merged.to_sql(table_name, conn, if_exists='replace', index=False, method='multi')
```

### Fixed `load_to_postgres.py`:
```python
# Drop table first
with engine.begin() as conn:
    conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))

# Write in separate transaction
with engine.begin() as conn:
    merged.to_sql(table_name, conn, if_exists='replace', 
                  index=False, method='multi', chunksize=1000)

# Verify in fresh connection
with engine.connect() as conn:
    result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).fetchone()
    # Validate result matches expected
```

## What the Fixes Address

1. **Explicit Transaction Control**: Drops existing table first, ensuring clean state
2. **Verification**: Confirms data was written with separate SELECT query
3. **Detailed Logging**: Shows exactly what's happening at each step
4. **Error Handling**: Better exception messages and stack traces
5. **Data Validation**: Checks file existence, column names, row counts

## Expected Output

When the fixed scripts run successfully, you should see:

```
🔗 Merging datasets...
  ✓ Loaded 100000 ratings
  ✓ Loaded 1682 movies
  ✓ Merged dataset: 100000 rows
💾 Loading to PostgreSQL...
  ✓ Dropped existing table (if any)
  ✓ Wrote 100000 rows to table: movie_ratings_merged
🔍 Verifying data was written...
  ✓ Table 'movie_ratings_merged' exists in database
  ✓ Verification: 100,000 rows in database
  ✅ Database load successful!
```

## Troubleshooting

### If you still don't see the table:

1. **Check Airflow logs**:
   ```bash
   make logs
   ```

2. **Run the test script**:
   ```bash
   docker exec -it airflow python /opt/airflow/scripts/test_pipeline.py
   ```

3. **Check database connection**:
   ```bash
   docker exec -it postgres psql -U airflow -d airflow -c "\dt"
   ```

4. **Verify files were downloaded**:
   ```bash
   docker exec -it airflow ls -lh /opt/airflow/data/ml-100k/ml-100k/
   ```

### Common Issues:

1. **Permission errors**: Run `chmod -R 777 data/ logs/` on host
2. **Database not ready**: Wait 60s after `make up` before running `make trigger`
3. **Old data cached**: Run `make clean-all && make build && make up`

## Testing the Fixes

To test without affecting your current setup:

```bash
# 1. Create test copies in the container
docker exec -it airflow cp /opt/airflow/scripts/transform_and_merge.py /opt/airflow/scripts/transform_and_merge.backup.py
docker exec -it airflow cp /opt/airflow/scripts/load_to_postgres.py /opt/airflow/scripts/load_to_postgres.backup.py

# 2. Copy fixed versions (after copying them to container)
docker cp transform_and_merge_fixed.py airflow:/opt/airflow/scripts/transform_and_merge.py
docker cp load_to_postgres_fixed.py airflow:/opt/airflow/scripts/load_to_postgres.py

# 3. Trigger DAG
make trigger

# 4. If it fails, restore backups
docker exec -it airflow cp /opt/airflow/scripts/transform_and_merge.backup.py /opt/airflow/scripts/transform_and_merge.py
docker exec -it airflow cp /opt/airflow/scripts/load_to_postgres.backup.py /opt/airflow/scripts/load_to_postgres.py
```

## Summary

These fixes provide:
- ✅ **Explicit transaction management** for reliable database writes
- ✅ **Comprehensive logging** for easier debugging
- ✅ **Data validation** at every step
- ✅ **Verification** that data was actually written
- ✅ **Test script** to run pipeline outside Airflow

The root cause was likely insufficient transaction handling and lack of verification. The fixed scripts explicitly manage transactions and verify data was committed to the database.