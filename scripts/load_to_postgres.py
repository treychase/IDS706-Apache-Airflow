import pandas as pd
from sqlalchemy import create_engine, text
import os
import sys

DB_CONN = os.getenv(
    'AIRFLOW__DATABASE__SQL_ALCHEMY_CONN',
    'postgresql+psycopg2://airflow:airflow@postgres:5432/airflow'
)


def merge_and_load(routes_csv, airports_csv):
    """
    Merge routes and airports datasets, then load to PostgreSQL with validation.
    
    Merges routes with airports TWICE:
    1. Once for source airport details
    2. Once for destination airport details
    
    Args:
        routes_csv: Path to processed routes CSV
        airports_csv: Path to processed airports CSV
    
    Returns:
        Boolean indicating success
    
    Raises:
        Exception if any step fails
    """
    print("\n" + "="*80)
    print("🔗 MERGING AND LOADING DATA")
    print("="*80)
    print(f"  🛫 Routes: {routes_csv}")
    print(f"  ✈️  Airports: {airports_csv}")
    
    # Verify input files exist
    from pathlib import Path
    if not Path(routes_csv).exists():
        raise FileNotFoundError(f"Routes file not found: {routes_csv}")
    if not Path(airports_csv).exists():
        raise FileNotFoundError(f"Airports file not found: {airports_csv}")
    
    # Read processed CSV files
    try:
        routes = pd.read_csv(routes_csv)
        print(f"  ✓ Loaded {len(routes)} routes")
    except Exception as e:
        raise ValueError(f"Failed to read routes CSV: {str(e)}")
    
    try:
        airports = pd.read_csv(airports_csv)
        print(f"  ✓ Loaded {len(airports)} airports")
    except Exception as e:
        raise ValueError(f"Failed to read airports CSV: {str(e)}")
    
    if len(routes) == 0:
        raise ValueError("Routes dataframe is empty!")
    if len(airports) == 0:
        raise ValueError("Airports dataframe is empty!")
    
    print(f"  ✓ Routes columns: {list(routes.columns)}")
    print(f"  ✓ Airports columns: {list(airports.columns)}")
    
    # Merge routes with source airports
    print(f"\n  🔗 Merging with source airports...")
    try:
        merged = routes.merge(
            airports,
            left_on='source_airport_id',
            right_on='airport_id',
            how='left',
            suffixes=('', '_src')
        )
    except Exception as e:
        raise ValueError(f"Failed to merge with source airports: {str(e)}")
    
    # Rename source airport columns
    merged = merged.rename(columns={
        'name': 'source_airport_name',
        'city': 'source_city',
        'country': 'source_country',
        'iata': 'source_iata',
        'icao': 'source_icao',
        'latitude': 'source_latitude',
        'longitude': 'source_longitude',
        'altitude': 'source_altitude'
    })
    
    # Drop the extra airport_id column from merge
    merged = merged.drop(columns=['airport_id'])
    
    print(f"  ✓ After source merge: {len(merged)} rows")
    
    # Merge with destination airports
    print(f"  🔗 Merging with destination airports...")
    try:
        merged = merged.merge(
            airports,
            left_on='dest_airport_id',
            right_on='airport_id',
            how='left',
            suffixes=('', '_dst')
        )
    except Exception as e:
        raise ValueError(f"Failed to merge with destination airports: {str(e)}")
    
    # Rename destination airport columns
    merged = merged.rename(columns={
        'name': 'dest_airport_name',
        'city': 'dest_city',
        'country': 'dest_country',
        'iata': 'dest_iata',
        'icao': 'dest_icao',
        'latitude': 'dest_latitude',
        'longitude': 'dest_longitude',
        'altitude': 'dest_altitude'
    })
    
    # Drop the extra airport_id column from merge
    merged = merged.drop(columns=['airport_id'])
    
    print(f"  ✓ After destination merge: {len(merged)} rows")
    print(f"  ✓ Final columns ({len(merged.columns)}): {list(merged.columns)}")
    
    # Verify merge quality
    null_source = merged['source_airport_name'].isna().sum()
    null_dest = merged['dest_airport_name'].isna().sum()
    
    print(f"\n  🔍 Data Quality Check:")
    print(f"     Missing source airports: {null_source}")
    print(f"     Missing dest airports: {null_dest}")
    
    if null_source > 0:
        print(f"  ⚠️  Warning: {null_source} routes have no matching source airport")
    if null_dest > 0:
        print(f"  ⚠️  Warning: {null_dest} routes have no matching destination airport")
    
    # Remove routes with missing airport data
    before_clean = len(merged)
    merged = merged.dropna(subset=['source_airport_name', 'dest_airport_name'])
    removed = before_clean - len(merged)
    print(f"  ✓ Removed {removed} incomplete routes")
    print(f"  ✓ Final clean dataset: {len(merged)} complete routes")
    
    if len(merged) == 0:
        raise ValueError("No complete routes remaining after merge!")
    
    # Connect to PostgreSQL and load data
    print(f"\n💾 LOADING TO POSTGRESQL")
    print("-"*80)
    db_info = DB_CONN.split('@')[1] if '@' in DB_CONN else 'unknown'
    print(f"  Database: {db_info}")
    
    try:
        engine = create_engine(DB_CONN)
    except Exception as e:
        raise ConnectionError(f"Failed to create database engine: {str(e)}")
    
    table_name = 'airport_routes_merged'
    
    try:
        # Test connection first
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print(f"  ✓ Database connection successful")
        
        # Drop existing table first (explicit transaction)
        print(f"  🗑️  Dropping existing table if exists...")
        with engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
        print(f"  ✓ Table dropped (if existed)")
        
        # Write to database in a new transaction
        print(f"  💾 Writing {len(merged)} rows to '{table_name}'...")
        with engine.begin() as conn:
            merged.to_sql(
                table_name, 
                conn, 
                if_exists='replace',
                index=False, 
                method='multi',
                chunksize=1000
            )
        print(f"  ✅ Write transaction committed")
        
        # Verify write in a separate transaction
        print(f"\n  🔍 VERIFYING DATA IN DATABASE")
        print("-"*80)
        with engine.connect() as conn:
            # Check table exists
            result = conn.execute(text(
                f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{table_name}')"
            )).fetchone()
            
            if not result[0]:
                raise ValueError(f"Table '{table_name}' was not created!")
            print(f"  ✓ Table '{table_name}' exists")
            
            # Count rows
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).fetchone()
            row_count = result[0]
            print(f"  ✓ Row count in database: {row_count:,}")
            
            if row_count != len(merged):
                raise ValueError(
                    f"Row count mismatch! Expected {len(merged):,}, got {row_count:,}"
                )
            
            # Get column names
            result = conn.execute(text(
                f"SELECT column_name FROM information_schema.columns "
                f"WHERE table_name = '{table_name}' ORDER BY ordinal_position"
            )).fetchall()
            
            db_columns = [row[0] for row in result]
            print(f"  ✓ Columns in database ({len(db_columns)}): {db_columns[:5]}... (showing first 5)")
            
            # Sample data
            result = conn.execute(text(f"SELECT * FROM {table_name} LIMIT 3")).fetchall()
            print(f"  ✓ Sample data retrieved: {len(result)} rows")
            
            # Show a sample row
            if result:
                sample = result[0]
                print(f"\n  📋 Sample Route:")
                print(f"     Airline: {sample[0] if len(sample) > 0 else 'N/A'}")
                print(f"     Source: {sample[6] if len(sample) > 6 else 'N/A'} ({sample[7] if len(sample) > 7 else 'N/A'})")
                if len(sample) > 14:
                    print(f"     Destination: {sample[14] if len(sample) > 14 else 'N/A'} ({sample[15] if len(sample) > 15 else 'N/A'})")
        
        print("\n✅ DATABASE LOAD SUCCESSFUL!")
        print("="*80)
        return True
        
    except Exception as e:
        print(f"\n❌ ERROR LOADING TO DATABASE: {str(e)}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == "__main__":
    # Test merge and load
    if len(sys.argv) > 2:
        try:
            routes_path = sys.argv[1]
            airports_path = sys.argv[2]
            success = merge_and_load(routes_path, airports_path)
            if success:
                print("\n✅ Merge and load test successful!")
                sys.exit(0)
        except Exception as e:
            print(f"\n❌ Merge and load test failed: {str(e)}")
            import traceback
            traceback.print_exc()
            sys.exit(1)
    else:
        print("Usage: python load_to_postgres.py <routes_csv> <airports_csv>")
        sys.exit(1)