import pandas as pd
from pathlib import Path

OUT_DIR = Path('/opt/airflow/data/processed')
OUT_DIR.mkdir(parents=True, exist_ok=True)


def transform_airports(airports_path):
    """
    Transform OpenFlights airports data.
    Input format: CSV with no header, 14 columns
    
    Columns:
    0: Airport ID, 1: Name, 2: City, 3: Country, 4: IATA, 5: ICAO,
    6: Latitude, 7: Longitude, 8: Altitude, 9: Timezone, 10: DST,
    11: Tz database, 12: Type, 13: Source
    """
    print(f"✈️  Transforming airports from: {airports_path}")
    
    # Column names for airports.dat
    cols = [
        'airport_id', 'name', 'city', 'country', 'iata', 'icao',
        'latitude', 'longitude', 'altitude', 'timezone', 'dst',
        'tz_database', 'type', 'source'
    ]
    
    df = pd.read_csv(airports_path, names=cols, na_values=['\\N'], encoding='utf-8')
    
    print(f"  ✓ Loaded {len(df)} airports")
    print(f"  ✓ Countries: {df['country'].nunique()}")
    print(f"  ✓ Columns: {list(df.columns)}")
    
    # Clean and filter
    df = df.dropna(subset=['airport_id', 'name'])
    df['airport_id'] = df['airport_id'].astype(int)
    
    # Convert numeric columns
    df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
    df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
    df['altitude'] = pd.to_numeric(df['altitude'], errors='coerce')
    
    # Remove rows with missing coordinates
    df = df.dropna(subset=['latitude', 'longitude'])
    
    # Select key columns for merging and analysis
    df = df[['airport_id', 'name', 'city', 'country', 'iata', 'icao', 
             'latitude', 'longitude', 'altitude']]
    
    print(f"  ✓ Cleaned dataset: {len(df)} airports with valid coordinates")
    print(f"  ✓ Sample airport: {df.iloc[0]['name']}, {df.iloc[0]['city']}, {df.iloc[0]['country']}")
    
    # Save transformed data
    out_path = OUT_DIR / 'airports_processed.csv'
    df.to_csv(out_path, index=False)
    
    print(f"  ✅ Saved {len(df)} processed airports to: {out_path}")
    return str(out_path)


def transform_routes(routes_path):
    """
    Transform OpenFlights routes data.
    Input format: CSV with no header, 9 columns
    
    Columns:
    0: Airline, 1: Airline ID, 2: Source airport, 3: Source airport ID,
    4: Dest airport, 5: Dest airport ID, 6: Codeshare, 7: Stops, 8: Equipment
    """
    print(f"🛫 Transforming routes from: {routes_path}")
    
    # Column names for routes.dat
    cols = [
        'airline', 'airline_id', 'source_airport', 'source_airport_id',
        'dest_airport', 'dest_airport_id', 'codeshare', 'stops', 'equipment'
    ]
    
    df = pd.read_csv(routes_path, names=cols, na_values=['\\N'], encoding='utf-8')
    
    print(f"  ✓ Loaded {len(df)} routes")
    print(f"  ✓ Unique airlines: {df['airline'].nunique()}")
    print(f"  ✓ Columns: {list(df.columns)}")
    
    # Clean data - need valid airport IDs for merging
    df = df.dropna(subset=['source_airport_id', 'dest_airport_id'])
    
    # Convert to numeric
    df['source_airport_id'] = pd.to_numeric(df['source_airport_id'], errors='coerce')
    df['dest_airport_id'] = pd.to_numeric(df['dest_airport_id'], errors='coerce')
    
    # Remove any rows that couldn't be converted
    df = df.dropna(subset=['source_airport_id', 'dest_airport_id'])
    df['source_airport_id'] = df['source_airport_id'].astype(int)
    df['dest_airport_id'] = df['dest_airport_id'].astype(int)
    
    # Convert stops to int
    df['stops'] = pd.to_numeric(df['stops'], errors='coerce').fillna(0).astype(int)
    
    # Fill missing airline names
    df['airline'] = df['airline'].fillna('Unknown')
    
    # Select key columns
    df = df[['airline', 'airline_id', 'source_airport_id', 'dest_airport_id', 
             'stops', 'equipment']]
    
    print(f"  ✓ Cleaned dataset: {len(df)} valid routes")
    print(f"  ✓ Direct flights (0 stops): {(df['stops'] == 0).sum()}")
    
    # Save transformed data
    out_path = OUT_DIR / 'routes_processed.csv'
    df.to_csv(out_path, index=False)
    
    print(f"  ✅ Saved {len(df)} processed routes to: {out_path}")
    return str(out_path)


if __name__ == "__main__":
    # Test transformation functions
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "airports":
            airports_path = sys.argv[2] if len(sys.argv) > 2 else "/opt/airflow/data/airports.dat"
            result = transform_airports(airports_path)
            print(f"\n✅ Airports transformed: {result}")
        
        elif sys.argv[1] == "routes":
            routes_path = sys.argv[2] if len(sys.argv) > 2 else "/opt/airflow/data/routes.dat"
            result = transform_routes(routes_path)
            print(f"\n✅ Routes transformed: {result}")
    else:
        print("Usage: python transform_and_merge.py [airports|routes] [file_path]")