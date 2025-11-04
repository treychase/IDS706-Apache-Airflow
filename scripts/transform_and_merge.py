import pandas as pd
from pathlib import Path
import sys

OUT_DIR = Path('/opt/airflow/data/processed')
OUT_DIR.mkdir(parents=True, exist_ok=True)


def transform_airports(airports_path):
    """
    Transform OpenFlights airports data with validation.
    Raises exceptions if transformation fails.
    """
    print("\n" + "="*80)
    print("✈️  TRANSFORMING AIRPORTS")
    print("="*80)
    print(f"Input: {airports_path}")
    
    # Verify input file exists and has content
    input_file = Path(airports_path)
    if not input_file.exists():
        raise FileNotFoundError(f"Airports file not found: {airports_path}")
    
    file_size = input_file.stat().st_size
    if file_size == 0:
        raise ValueError(f"Airports file is empty: {airports_path}")
    
    print(f"  ✓ Input file size: {file_size:,} bytes")
    
    # Column names for airports.dat
    cols = [
        'airport_id', 'name', 'city', 'country', 'iata', 'icao',
        'latitude', 'longitude', 'altitude', 'timezone', 'dst',
        'tz_database', 'type', 'source'
    ]
    
    try:
        df = pd.read_csv(airports_path, names=cols, na_values=['\\N'], encoding='utf-8')
    except Exception as e:
        raise ValueError(f"Failed to read airports CSV: {str(e)}")
    
    print(f"  ✓ Loaded {len(df)} airports")
    
    if len(df) == 0:
        raise ValueError("No airports loaded from file!")
    
    print(f"  ✓ Countries: {df['country'].nunique()}")
    print(f"  ✓ Columns: {list(df.columns)}")
    
    # Clean and filter
    initial_count = len(df)
    df = df.dropna(subset=['airport_id', 'name'])
    print(f"  ✓ After removing rows without ID/name: {len(df)} ({initial_count - len(df)} removed)")
    
    if len(df) == 0:
        raise ValueError("All airports removed after cleaning!")
    
    df['airport_id'] = df['airport_id'].astype(int)
    
    # Convert numeric columns
    df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
    df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
    df['altitude'] = pd.to_numeric(df['altitude'], errors='coerce')
    
    # Remove rows with missing coordinates
    before_coords = len(df)
    df = df.dropna(subset=['latitude', 'longitude'])
    print(f"  ✓ After removing rows without coordinates: {len(df)} ({before_coords - len(df)} removed)")
    
    if len(df) == 0:
        raise ValueError("All airports removed after coordinate filtering!")
    
    # Select key columns for merging and analysis
    df = df[['airport_id', 'name', 'city', 'country', 'iata', 'icao', 
             'latitude', 'longitude', 'altitude']]
    
    print(f"  ✓ Final dataset: {len(df)} airports with valid coordinates")
    print(f"  ✓ Sample: {df.iloc[0]['name']}, {df.iloc[0]['city']}, {df.iloc[0]['country']}")
    
    # Save transformed data
    out_path = OUT_DIR / 'airports_processed.csv'
    try:
        df.to_csv(out_path, index=False)
    except Exception as e:
        raise IOError(f"Failed to write airports CSV: {str(e)}")
    
    # Verify output file
    if not out_path.exists():
        raise FileNotFoundError(f"Output file was not created: {out_path}")
    
    out_size = out_path.stat().st_size
    if out_size == 0:
        raise ValueError(f"Output file is empty: {out_path}")
    
    print(f"  ✅ Saved to: {out_path}")
    print(f"  ✅ Output size: {out_size:,} bytes")
    print("="*80)
    
    return str(out_path)


def transform_routes(routes_path):
    """
    Transform OpenFlights routes data with validation.
    Raises exceptions if transformation fails.
    """
    print("\n" + "="*80)
    print("🛫 TRANSFORMING ROUTES")
    print("="*80)
    print(f"Input: {routes_path}")
    
    # Verify input file exists and has content
    input_file = Path(routes_path)
    if not input_file.exists():
        raise FileNotFoundError(f"Routes file not found: {routes_path}")
    
    file_size = input_file.stat().st_size
    if file_size == 0:
        raise ValueError(f"Routes file is empty: {routes_path}")
    
    print(f"  ✓ Input file size: {file_size:,} bytes")
    
    # Column names for routes.dat
    cols = [
        'airline', 'airline_id', 'source_airport', 'source_airport_id',
        'dest_airport', 'dest_airport_id', 'codeshare', 'stops', 'equipment'
    ]
    
    try:
        df = pd.read_csv(routes_path, names=cols, na_values=['\\N'], encoding='utf-8')
    except Exception as e:
        raise ValueError(f"Failed to read routes CSV: {str(e)}")
    
    print(f"  ✓ Loaded {len(df)} routes")
    
    if len(df) == 0:
        raise ValueError("No routes loaded from file!")
    
    print(f"  ✓ Unique airlines: {df['airline'].nunique()}")
    print(f"  ✓ Columns: {list(df.columns)}")
    
    # Clean data - need valid airport IDs for merging
    initial_count = len(df)
    df = df.dropna(subset=['source_airport_id', 'dest_airport_id'])
    print(f"  ✓ After removing routes without airport IDs: {len(df)} ({initial_count - len(df)} removed)")
    
    if len(df) == 0:
        raise ValueError("All routes removed after cleaning!")
    
    # Convert to numeric
    df['source_airport_id'] = pd.to_numeric(df['source_airport_id'], errors='coerce')
    df['dest_airport_id'] = pd.to_numeric(df['dest_airport_id'], errors='coerce')
    
    # Remove any rows that couldn't be converted
    before_convert = len(df)
    df = df.dropna(subset=['source_airport_id', 'dest_airport_id'])
    print(f"  ✓ After removing non-numeric IDs: {len(df)} ({before_convert - len(df)} removed)")
    
    if len(df) == 0:
        raise ValueError("All routes removed after numeric conversion!")
    
    df['source_airport_id'] = df['source_airport_id'].astype(int)
    df['dest_airport_id'] = df['dest_airport_id'].astype(int)
    
    # Convert stops to int
    df['stops'] = pd.to_numeric(df['stops'], errors='coerce').fillna(0).astype(int)
    
    # Fill missing airline names
    df['airline'] = df['airline'].fillna('Unknown')
    
    # Select key columns
    df = df[['airline', 'airline_id', 'source_airport_id', 'dest_airport_id', 
             'stops', 'equipment']]
    
    print(f"  ✓ Final dataset: {len(df)} valid routes")
    print(f"  ✓ Direct flights (0 stops): {(df['stops'] == 0).sum()}")
    
    # Save transformed data
    out_path = OUT_DIR / 'routes_processed.csv'
    try:
        df.to_csv(out_path, index=False)
    except Exception as e:
        raise IOError(f"Failed to write routes CSV: {str(e)}")
    
    # Verify output file
    if not out_path.exists():
        raise FileNotFoundError(f"Output file was not created: {out_path}")
    
    out_size = out_path.stat().st_size
    if out_size == 0:
        raise ValueError(f"Output file is empty: {out_path}")
    
    print(f"  ✅ Saved to: {out_path}")
    print(f"  ✅ Output size: {out_size:,} bytes")
    print("="*80)
    
    return str(out_path)


if __name__ == "__main__":
    # Test transformation functions
    if len(sys.argv) > 1:
        try:
            if sys.argv[1] == "airports":
                airports_path = sys.argv[2] if len(sys.argv) > 2 else "/opt/airflow/data/airports.dat"
                result = transform_airports(airports_path)
                print(f"\n✅ Airports transformed successfully: {result}")
                sys.exit(0)
            
            elif sys.argv[1] == "routes":
                routes_path = sys.argv[2] if len(sys.argv) > 2 else "/opt/airflow/data/routes.dat"
                result = transform_routes(routes_path)
                print(f"\n✅ Routes transformed successfully: {result}")
                sys.exit(0)
                
        except Exception as e:
            print(f"\n❌ Transformation failed: {str(e)}")
            import traceback
            traceback.print_exc()
            sys.exit(1)
    else:
        print("Usage: python transform_and_merge.py [airports|routes] [file_path]")
        sys.exit(1)