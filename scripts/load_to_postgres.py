import pandas as pd
from sqlalchemy import create_engine, text
import os

DB_CONN = os.getenv(
    'AIRFLOW__DATABASE__SQL_ALCHEMY_CONN',
    'postgresql+psycopg2://airflow:airflow@postgres:5432/airflow'
)


def merge_and_load(routes_csv, airports_csv):
    """
    Merge routes and airports datasets, then load to PostgreSQL.
    
    Merges routes with airports TWICE:
    1. Once for source airport details
    2. Once for destination airport details
    
    Args:
        routes_csv: Path to processed routes CSV
        airports_csv: Path to processed airports CSV
    
    Returns:
        Boolean indicating success
    """
    print(f"🔗 Merging datasets...")
    print(f"  🛫 Routes: {routes_csv}")
    print(f"  ✈️  Airports: {airports_csv}")
    
    # Read processed CSV files
    routes = pd.read_csv(routes_csv)
    airports = pd.read_csv(airports_csv)
    
    print(f"  ✓ Loaded {len(routes)} routes")
    print(f"  ✓ Loaded {len(airports)} airports")
    
    # Merge routes with source airports
    print(f"  🔗 Merging with source airports...")
    merged = routes.merge(
        airports,
        left_on='source_airport_id',
        right_on='airport_id',
        how='left',
        suffixes=('', '_src')
    )
    
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
    
    print(f"  ✓ Source airports merged: {len(merged)} rows")
    
    # Merge with destination airports
    print(f"  🔗 Merging with destination airports...")
    merged = merged.merge(
        airports,
        left_on='dest_airport_id',
        right_on='airport_id',
        how='left',
        suffixes=('', '_dst')
    )
    
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
    
    print(f"  ✓ Destination airports merged: {len(merged)} rows")
    print(f"  ✓ Final columns: {list(merged.columns)}")
    
    # Verify merge quality
    null_source = merged['source_airport_name'].isna().sum()
    null_dest = merged['dest_airport_name'].isna().sum()
    
    if null_source > 0:
        print(f"  ⚠️  Warning: {null_source} routes have no matching source airport")
    if null_dest > 0:
        print(f"  ⚠️  Warning: {null_dest} routes have no matching destination airport")
    
    # Remove routes with missing airport data
    merged = merged.dropna(subset=['source_airport_name', 'dest_airport_name'])
    print(f"  ✓ After cleaning: {len(merged)} complete routes")
    
    # Connect to PostgreSQL and load data
    print(f"💾 Loading to PostgreSQL...")
    print(f"  Database: {DB_CONN.split('@')[1] if '@' in DB_CONN else 'unknown'}")
    
    engine = create_engine(DB_CONN)
    
    # Write to database (replace if exists) with explicit connection and transaction
    table_name = 'airport_routes_merged'
    
    try:
        # Use begin() for automatic transaction commit
        with engine.begin() as conn:
            merged.to_sql(table_name, conn, if_exists='replace', index=False, method='multi')
            print(f"  ✅ Wrote {len(merged)} rows to table: {table_name}")
        
        # Verify write in a separate transaction to ensure data is committed
        print(f"  🔍 Verifying data was written...")
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).fetchone()
            row_count = result[0]
            print(f"  ✓ Verification: {row_count} rows in database")
            
            if row_count != len(merged):
                raise ValueError(f"Row count mismatch! Expected {len(merged)}, got {row_count}")
            
            # Also verify we can read some sample data
            sample = conn.execute(text(f"SELECT * FROM {table_name} LIMIT 5")).fetchall()
            print(f"  ✓ Sample data retrieved: {len(sample)} rows")
        
        print(f"  ✅ Database load successful!")
        return True
        
    except Exception as e:
        print(f"  ❌ Error loading to database: {str(e)}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == "__main__":
    # Test merge and load
    import sys
    
    if len(sys.argv) > 2:
        routes_path = sys.argv[1]
        airports_path = sys.argv[2]
        success = merge_and_load(routes_path, airports_path)
        if success:
            print("\n✅ Merge and load successful!")
    else:
        print("Usage: python load_to_postgres.py <routes_csv> <airports_csv>")