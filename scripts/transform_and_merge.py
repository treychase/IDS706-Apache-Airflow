import pandas as pd
from pathlib import Path

OUT_DIR = Path('/opt/airflow/data/processed')
OUT_DIR.mkdir(parents=True, exist_ok=True)

def transform_ratings(ratings_path):
    cols = ['userId', 'movieId', 'rating', 'timestamp']
    df = pd.read_csv(ratings_path, sep='\t', names=cols, engine='python')
    out_path = OUT_DIR / 'ratings_processed.csv'
    df.to_csv(out_path, index=False)
    return str(out_path)

def transform_movies(movies_path):
    raw = pd.read_csv(movies_path, sep='|', header=None, encoding='latin-1')
    df = pd.DataFrame({
        'movieId': raw[0].astype(int),
        'title': raw[1],
        'genres': raw.iloc[:, 5:].apply(lambda row: '|'.join([str(x) for x in row if x == 1 or str(x).isdigit() == False and x != '0' and x != 0]), axis=1)
    })
    df = df[['movieId', 'title', 'genres']]
    out_path = OUT_DIR / 'movies_processed.csv'
    df.to_csv(out_path, index=False)
    return str(out_path)
