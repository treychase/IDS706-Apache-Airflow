import pandas as pd
from pathlib import Path

OUT_DIR = Path('/opt/airflow/data/processed')
OUT_DIR.mkdir(parents=True, exist_ok=True)


def transform_ratings(ratings_path):
    """
    Transform MovieLens ratings data.
    Input format: tab-separated (userId, movieId, rating, timestamp)
    """
    print(f"📊 Transforming ratings from: {ratings_path}")
    
    # Read tab-separated file with proper column names
    cols = ['userId', 'movieId', 'rating', 'timestamp']
    df = pd.read_csv(ratings_path, sep='\t', names=cols, engine='python')
    
    print(f"  ✓ Loaded {len(df)} ratings")
    print(f"  ✓ Columns: {list(df.columns)}")
    print(f"  ✓ Rating range: {df['rating'].min()} to {df['rating'].max()}")
    
    # Basic data quality checks
    df = df.dropna()  # Remove any null values
    df['userId'] = df['userId'].astype(int)
    df['movieId'] = df['movieId'].astype(int)
    df['rating'] = df['rating'].astype(float)
    df['timestamp'] = df['timestamp'].astype(int)
    
    # Save transformed data
    out_path = OUT_DIR / 'ratings_processed.csv'
    df.to_csv(out_path, index=False)
    
    print(f"  ✅ Saved {len(df)} processed ratings to: {out_path}")
    return str(out_path)


def transform_movies(movies_path):
    """
    Transform MovieLens movies data.
    Input format: pipe-separated with 24 columns
    (movieId|title|release_date|video_release_date|IMDb_URL|genre_columns...)
    """
    print(f"🎬 Transforming movies from: {movies_path}")
    
    # Read pipe-separated file
    # The file has no header and genres are in columns 5-23 (19 genre columns)
    df = pd.read_csv(movies_path, sep='|', header=None, encoding='latin-1')
    
    print(f"  ✓ Loaded {len(df)} movies")
    print(f"  ✓ Total columns: {len(df.columns)}")
    
    # Extract movie ID, title, and genres
    # Columns 0: movieId, 1: title, 5-23: genre indicators
    genre_names = [
        'unknown', 'Action', 'Adventure', 'Animation', 'Children',
        'Comedy', 'Crime', 'Documentary', 'Drama', 'Fantasy',
        'Film-Noir', 'Horror', 'Musical', 'Mystery', 'Romance',
        'Sci-Fi', 'Thriller', 'War', 'Western'
    ]
    
    # Build genres string for each movie
    genres_list = []
    for idx, row in df.iterrows():
        movie_genres = []
        for i, genre_name in enumerate(genre_names):
            if row[5 + i] == 1:  # Genre columns start at index 5
                movie_genres.append(genre_name)
        genres_list.append('|'.join(movie_genres) if movie_genres else 'unknown')
    
    # Create clean dataframe
    movies_df = pd.DataFrame({
        'movieId': df[0].astype(int),
        'title': df[1].astype(str),
        'genres': genres_list
    })
    
    print(f"  ✓ Processed columns: {list(movies_df.columns)}")
    print(f"  ✓ Sample title: {movies_df['title'].iloc[0]}")
    print(f"  ✓ Sample genres: {movies_df['genres'].iloc[0]}")
    
    # Save transformed data
    out_path = OUT_DIR / 'movies_processed.csv'
    movies_df.to_csv(out_path, index=False)
    
    print(f"  ✅ Saved {len(movies_df)} processed movies to: {out_path}")
    return str(out_path)


if __name__ == "__main__":
    # Test transformation functions
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "ratings":
            ratings_path = sys.argv[2] if len(sys.argv) > 2 else "/opt/airflow/data/ml-100k/ml-100k/u.data"
            result = transform_ratings(ratings_path)
            print(f"\n✅ Ratings transformed: {result}")
        
        elif sys.argv[1] == "movies":
            movies_path = sys.argv[2] if len(sys.argv) > 2 else "/opt/airflow/data/ml-100k/ml-100k/u.item"
            result = transform_movies(movies_path)
            print(f"\n✅ Movies transformed: {result}")
    else:
        print("Usage: python transform_and_merge.py [ratings|movies] [file_path]")