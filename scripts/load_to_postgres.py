import pandas as pd
from sqlalchemy import create_engine, text
import os

DB_CONN = os.getenv(
    'AIRFLOW__DATABASE__SQL_ALCHEMY_CONN',
    'postgresql+psycopg2://airflow:airflow@postgres:5432/airflow'
)


def merge_and_load(ratings_csv, movies_csv):
    """
    Merge ratings and movies datasets, then load to PostgreSQL.
    
    Args:
        ratings_csv: Path to processed ratings CSV
        movies_csv: Path to processed movies CSV
    
    Returns:
        Boolean indicating success
    """
    print(f"🔗 Merging datasets...")
    print(f"  📊 Ratings: {ratings_csv}")
    print(f"  🎬 Movies: {movies_csv}")
    
    # Read processed CSV files
    ratings = pd.read_csv(ratings_csv)
    movies = pd.read_csv(movies_csv)
    
    print(f"  ✓ Loaded {len(ratings)} ratings")
    print(f"  ✓ Loaded {len(movies)} movies")
    
    # Merge on movieId
    merged = ratings.merge(movies, on='movieId', how='left')
    
    print(f"  ✓ Merged dataset: {len(merged)} rows")
    print(f"  ✓ Columns: {list(merged.columns)}")
    
    # Verify merge quality
    null_titles = merged['title'].isna().sum()
    if null_titles > 0:
        print(f"  ⚠️  Warning: {null_titles} ratings have no matching movie")
    
    # Connect to PostgreSQL and load data
    print(f"💾 Loading to PostgreSQL...")
    print(f"  Database: {DB_CONN.split('@')[1] if '@' in DB_CONN else 'unknown'}")
    
    engine = create_engine(DB_CONN)
    
    # Write to database (replace if exists) with explicit connection and transaction
    table_name = 'movie_ratings_merged'
    
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
        ratings_path = sys.argv[1]
        movies_path = sys.argv[2]
        success = merge_and_load(ratings_path, movies_path)
        if success:
            print("\n✅ Merge and load successful!")
    else:
        print("Usage: python load_to_postgres.py <ratings_csv> <movies_csv>")