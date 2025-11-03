import requests, zipfile, io, os
from pathlib import Path

DATA_DIR = Path('/opt/airflow/data')
DATA_DIR.mkdir(parents=True, exist_ok=True)

MOVIELENS_100K_URL = "http://files.grouplens.org/datasets/movielens/ml-100k.zip"

def download_movielens_100k():
    local_zip = DATA_DIR / 'ml-100k.zip'
    if not local_zip.exists():
        print("Downloading MovieLens 100K...")
        r = requests.get(MOVIELENS_100K_URL, stream=True)
        r.raise_for_status()
        with open(local_zip, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    else:
        print("Using cached ml-100k.zip")
    with zipfile.ZipFile(local_zip, 'r') as z:
        z.extractall(DATA_DIR / 'ml-100k')

    ratings = DATA_DIR / 'ml-100k' / 'u.data'
    movies = DATA_DIR / 'ml-100k' / 'u.item'
    return {'ratings': str(ratings), 'movies': str(movies)}
