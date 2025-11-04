import requests
from pathlib import Path
import sys

DATA_DIR = Path('/opt/airflow/data')
DATA_DIR.mkdir(parents=True, exist_ok=True)

AIRPORTS_URL = "https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports.dat"
ROUTES_URL = "https://raw.githubusercontent.com/jpatokal/openflights/master/data/routes.dat"

def download_openflights():
    """
    Downloads OpenFlights airport and route datasets with validation.
    Returns dictionary with paths to airports and routes files.
    Raises exceptions if download or validation fails.
    """
    print("="*80)
    print("📥 DOWNLOADING OPENFLIGHTS DATA")
    print("="*80)
    
    airports_file = DATA_DIR / 'airports.dat'
    routes_file = DATA_DIR / 'routes.dat'
    
    # Always re-download to ensure fresh data
    try:
        # Download airports
        print(f"\n📥 Downloading airports from {AIRPORTS_URL}")
        response = requests.get(AIRPORTS_URL, timeout=30)
        response.raise_for_status()
        
        # Validate content
        if len(response.text) < 1000:
            raise ValueError(f"Downloaded airports file too small: {len(response.text)} bytes")
        
        airports_file.write_text(response.text, encoding='utf-8')
        print(f"  ✅ Downloaded {len(response.text):,} bytes")
        
        # Verify file was written
        if not airports_file.exists():
            raise FileNotFoundError(f"Failed to write airports file to {airports_file}")
        
        actual_size = airports_file.stat().st_size
        print(f"  ✅ Verified file on disk: {actual_size:,} bytes")
        
        # Count lines to verify data
        line_count = len(airports_file.read_text(encoding='utf-8').splitlines())
        print(f"  ✅ File contains {line_count:,} lines")
        
        if line_count < 100:
            raise ValueError(f"Airports file has too few lines: {line_count}")
            
    except Exception as e:
        print(f"  ❌ ERROR downloading airports: {str(e)}")
        raise
    
    try:
        # Download routes
        print(f"\n📥 Downloading routes from {ROUTES_URL}")
        response = requests.get(ROUTES_URL, timeout=30)
        response.raise_for_status()
        
        # Validate content
        if len(response.text) < 1000:
            raise ValueError(f"Downloaded routes file too small: {len(response.text)} bytes")
        
        routes_file.write_text(response.text, encoding='utf-8')
        print(f"  ✅ Downloaded {len(response.text):,} bytes")
        
        # Verify file was written
        if not routes_file.exists():
            raise FileNotFoundError(f"Failed to write routes file to {routes_file}")
        
        actual_size = routes_file.stat().st_size
        print(f"  ✅ Verified file on disk: {actual_size:,} bytes")
        
        # Count lines to verify data
        line_count = len(routes_file.read_text(encoding='utf-8').splitlines())
        print(f"  ✅ File contains {line_count:,} lines")
        
        if line_count < 100:
            raise ValueError(f"Routes file has too few lines: {line_count}")
            
    except Exception as e:
        print(f"  ❌ ERROR downloading routes: {str(e)}")
        raise
    
    # Final validation
    print("\n🔍 FINAL VALIDATION")
    print("-"*80)
    
    if not airports_file.exists():
        raise FileNotFoundError(f"Airports file not found: {airports_file}")
    if not routes_file.exists():
        raise FileNotFoundError(f"Routes file not found: {routes_file}")
    
    airports_size = airports_file.stat().st_size
    routes_size = routes_file.stat().st_size
    
    print(f"  ✅ Airports file: {airports_file}")
    print(f"     Size: {airports_size:,} bytes")
    print(f"  ✅ Routes file: {routes_file}")
    print(f"     Size: {routes_size:,} bytes")
    
    if airports_size == 0 or routes_size == 0:
        raise ValueError("One or more downloaded files are empty!")
    
    print("\n✅ DOWNLOAD SUCCESSFUL!")
    print("="*80)
    
    return {
        'airports': str(airports_file),
        'routes': str(routes_file)
    }


if __name__ == "__main__":
    # Test the download function
    try:
        paths = download_openflights()
        print(f"\n✅ Download test successful!")
        print(f"Airports: {paths['airports']}")
        print(f"Routes: {paths['routes']}")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Download test failed: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)