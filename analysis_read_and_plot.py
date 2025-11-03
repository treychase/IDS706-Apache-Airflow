import pandas as pd
from sqlalchemy import create_engine
import os
import matplotlib.pyplot as plt

DB_CONN = os.getenv('AIRFLOW__DATABASE__SQL_ALCHEMY_CONN',
                    'postgresql+psycopg2://airflow:airflow@postgres:5432/airflow')

OUT_DIR = '/opt/airflow/data/analysis'
os.makedirs(OUT_DIR, exist_ok=True)

def run_analysis():
    engine = create_engine(DB_CONN)
    df = pd.read_sql_table('movie_ratings_merged', engine)
    agg = df.groupby(['movieId', 'title']).rating.agg(['mean', 'count']).reset_index()
    top = agg[agg['count'] >= 10].sort_values('mean', ascending=False).head(10)
    print("Top rated movies (at least 10 ratings):")
    print(top[['title','mean','count']])
    plt.figure(figsize=(10,6))
    plt.barh(top['title'], top['mean'])
    plt.xlabel('Average rating')
    plt.title('Top 10 average-rated movies (min 10 ratings)')
    plt.tight_layout()
    out_path = os.path.join(OUT_DIR, 'top10_avg_rated_movies.png')
    plt.savefig(out_path)
    print(f"Saved plot to {out_path}")
