#!/usr/bin/env python3
"""
Debug script to check the state of the MovieLens database tables.
Run this to see what tables exist and what data they contain.
"""

import os
from sqlalchemy import create_engine, text, inspect
import pandas as pd

DB_CONN = os.getenv(
    'AIRFLOW__DATABASE__SQL_ALCHEMY_CONN',
    'postgresql+psycopg2://airflow:airflow@postgres:5432/airflow'
)

def debug_database():
    print("🔍 MovieLens Database Debugger")
    print("=" * 70)
    
    engine = create_engine(DB_CONN)
    
    print(f"\n📊 Connecting to: {DB_CONN.split('@')[1] if '@' in DB_CONN else DB_CONN}")
    
    try:
        with engine.connect() as conn:
            # Get list of all tables
            inspector = inspect(engine)
            all_tables = inspector.get_table_names()
            
            print(f"\n📋 All tables in database ({len(all_tables)} total):")
            for table in all_tables:
                print(f"  • {table}")
            
            # Check specifically for our table
            target_table = 'movie_ratings_merged'
            print(f"\n🎯 Checking for target table: '{target_table}'")
            
            if target_table in all_tables:
                print(f"  ✅ Table '{target_table}' EXISTS")
                
                # Get row count
                result = conn.execute(text(f"SELECT COUNT(*) FROM {target_table}")).fetchone()
                row_count = result[0]
                print(f"  📊 Row count: {row_count:,}")
                
                if row_count > 0:
                    # Get column names
                    columns = inspector.get_columns(target_table)
                    print(f"\n  📝 Columns ({len(columns)}):")
                    for col in columns:
                        print(f"    • {col['name']} ({col['type']})")
                    
                    # Get sample data
                    print(f"\n  📊 Sample data (first 5 rows):")
                    sample_df = pd.read_sql_query(f"SELECT * FROM {target_table} LIMIT 5", conn)
                    print(sample_df.to_string(index=False))
                    
                    # Get some statistics
                    print(f"\n  📈 Statistics:")
                    stats = conn.execute(text(f"""
                        SELECT 
                            COUNT(DISTINCT "userId") as unique_users,
                            COUNT(DISTINCT "movieId") as unique_movies,
                            AVG(rating) as avg_rating,
                            MIN(rating) as min_rating,
                            MAX(rating) as max_rating
                        FROM {target_table}
                    """)).fetchone()
                    
                    print(f"    • Unique users: {stats[0]:,}")
                    print(f"    • Unique movies: {stats[1]:,}")
                    print(f"    • Average rating: {stats[2]:.2f}")
                    print(f"    • Rating range: {stats[3]} - {stats[4]}")
                else:
                    print(f"  ⚠️  Table exists but is EMPTY")
            else:
                print(f"  ❌ Table '{target_table}' DOES NOT EXIST")
                print(f"\n  💡 Possible reasons:")
                print(f"    1. The merge_and_load task hasn't run yet")
                print(f"    2. The merge_and_load task failed")
                print(f"    3. Transaction wasn't committed")
                print(f"\n  🔧 Suggested actions:")
                print(f"    1. Check Airflow task logs for merge_and_load_task")
                print(f"    2. Make sure the DAG completed successfully")
                print(f"    3. Try running: make trigger")
            
            # Check for intermediate tables that might have been created
            print(f"\n🔍 Checking for related tables:")
            related_tables = [t for t in all_tables if 'movie' in t.lower() or 'rating' in t.lower()]
            if related_tables:
                for table in related_tables:
                    result = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).fetchone()
                    print(f"  • {table}: {result[0]:,} rows")
            else:
                print(f"  ℹ️  No movie or rating related tables found")
    
    except Exception as e:
        print(f"\n❌ Error connecting to database: {str(e)}")
        import traceback
        traceback.print_exc()
        return False
    
    print("\n" + "=" * 70)
    print("✅ Debug complete!")
    return True


if __name__ == "__main__":
    debug_database()