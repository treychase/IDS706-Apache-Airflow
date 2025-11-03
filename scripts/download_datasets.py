import requests
import zipfile
import os
from pathlib import Path

DATA_DIR = Path('/opt/airflow/data')
DATA_DIR.mkdir(parents=True, exist_ok=True)

MOVIELENS_100K_URL = "http://files.grouplens.org/datasets/movielens/ml-100k.zip"

def download_movielens_100k():
    """
    Downloads and extracts MovieLens 100K dataset.
    Returns dictionary with paths to ratings and movies files.
    """
    local_zip = DATA_DIR / 'ml-100k.zip'
    extract_dir = DATA_DIR / 'ml-100k'
    
    # Download if not cached
    if not local_zip.exists():
        print(f"📥 Downloading MovieLens 100K from {MOVIELENS_100K_URL}")
        r = requests.get(MOVIELENS_100K_URL, stream=True)
        r.raise_for_status()
        with open(local_zip, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"✅ Downloaded to {local_zip}")
    else:
        print(f"✅ Using cached {local_zip}")
    
    # Extract
    if not extract_dir.exists():
        print(f"📦 Extracting to {extract_dir}")
        with zipfile.ZipFile(local_zip, 'r') as z:
            z.extractall(DATA_DIR)
        print("✅ Extraction complete")
    else:
        print(f"✅ Already extracted to {extract_dir}")

    # Verify files exist
    ratings_file = extract_dir / 'ml-100k' / 'u.data'
    movies_file = extract_dir / 'ml-100k' / 'u.item'
    
    if not ratings_file.exists():
        raise FileNotFoundError(f"Ratings file not found: {ratings_file}")
    if not movies_file.exists():
        raise FileNotFoundError(f"Movies file not found: {movies_file}")
    
    print(f"📊 Ratings file: {ratings_file}")
    print(f"🎬 Movies file: {movies_file}")
    
    return {
        'ratings': str(ratings_file),
        'movies': str(movies_file)
    }


if __name__ == "__main__":
    # Test the download function
    paths = download_movielens_100k()
    print(f"\n✅ Download successful!")
    print(f"Ratings: {paths['ratings']}")
    print(f"Movies: {paths['movies']}")