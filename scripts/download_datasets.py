import requests
from pathlib import Path

DATA_DIR = Path('/opt/airflow/data')
DATA_DIR.mkdir(parents=True, exist_ok=True)

AIRPORTS_URL = "https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports.dat"
ROUTES_URL = "https://raw.githubusercontent.com/jpatokal/openflights/master/data/routes.dat"

def download_openflights():
    """
    Downloads OpenFlights airport and route datasets.
    Returns dictionary with paths to airports and routes files.
    """
    airports_file = DATA_DIR / 'airports.dat'
    routes_file = DATA_DIR / 'routes.dat'
    
    # Download airports
    if not airports_file.exists():
        print(f"📥 Downloading airports from {AIRPORTS_URL}")
        r = requests.get(AIRPORTS_URL)
        r.raise_for_status()
        airports_file.write_text(r.text, encoding='utf-8')
        print(f"✅ Downloaded to {airports_file}")
    else:
        print(f"✅ Using cached {airports_file}")
    
    # Download routes
    if not routes_file.exists():
        print(f"📥 Downloading routes from {ROUTES_URL}")
        r = requests.get(ROUTES_URL)
        r.raise_for_status()
        routes_file.write_text(r.text, encoding='utf-8')
        print(f"✅ Downloaded to {routes_file}")
    else:
        print(f"✅ Using cached {routes_file}")
    
    # Verify files exist
    if not airports_file.exists():
        raise FileNotFoundError(f"Airports file not found: {airports_file}")
    if not routes_file.exists():
        raise FileNotFoundError(f"Routes file not found: {routes_file}")
    
    print(f"✈️  Airports file: {airports_file}")
    print(f"🛫 Routes file: {routes_file}")
    
    return {
        'airports': str(airports_file),
        'routes': str(routes_file)
    }


if __name__ == "__main__":
    # Test the download function
    paths = download_openflights()
    print(f"\n✅ Download successful!")
    print(f"Airports: {paths['airports']}")
    print(f"Routes: {paths['routes']}")