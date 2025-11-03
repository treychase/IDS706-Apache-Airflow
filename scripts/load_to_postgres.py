import pandas as pd
from sqlalchemy import create_engine
import os

DB_CONN = os.getenv('AIRFLOW__DATABASE__SQL_ALCHEMY_CONN',
                    'postgresql+psycopg2://airflow:airflow@postgres:5432/airflow')

def merge_and_load(ratings_csv, movies_csv):
    r = pd.read_csv(ratings_csv)
    m = pd.read_csv(movies_csv)
    merged = r.merge(m, on='movieId', how='left')
    engine = create_engine(DB_CONN)
    merged.to_sql('movie_ratings_merged', engine, if_exists='replace', index=False)
    print(f"Wrote merged table to DB: movie_ratings_merged (rows={len(merged)})")
    return True
