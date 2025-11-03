import pandas as pd
from sqlalchemy import create_engine
import os
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend for server environments
import matplotlib.pyplot as plt

DB_CONN = os.getenv(
    'AIRFLOW__DATABASE__SQL_ALCHEMY_CONN',
    'postgresql+psycopg2://airflow:airflow@postgres:5432/airflow'
)

OUT_DIR = '/opt/airflow/data/analysis'
os.makedirs(OUT_DIR, exist_ok=True)


def run_analysis():
    """
    Read data from PostgreSQL and perform analysis.
    Creates visualization of top-rated movies.
    """
    print("📊 Running analysis...")
    print(f"  Database: {DB_CONN.split('@')[1] if '@' in DB_CONN else 'unknown'}")
    
    # Connect to database
    engine = create_engine(DB_CONN)
    
    # Read merged data from database
    table_name = 'movie_ratings_merged'
    print(f"  📖 Reading from table: {table_name}")
    
    df = pd.read_sql_table(table_name, engine)
    print(f"  ✓ Loaded {len(df)} rows from database")
    print(f"  ✓ Columns: {list(df.columns)}")
    
    # Aggregate ratings by movie
    print("  📈 Aggregating ratings by movie...")
    agg = df.groupby(['movieId', 'title']).agg({
        'rating': ['mean', 'count', 'std']
    }).reset_index()
    
    # Flatten column names
    agg.columns = ['movieId', 'title', 'mean_rating', 'rating_count', 'rating_std']
    
    # Filter movies with at least 10 ratings for reliability
    min_ratings = 10
    agg_filtered = agg[agg['rating_count'] >= min_ratings]
    
    print(f"  ✓ {len(agg)} total movies")
    print(f"  ✓ {len(agg_filtered)} movies with ≥{min_ratings} ratings")
    
    # Get top 10 by average rating
    top_movies = agg_filtered.sort_values('mean_rating', ascending=False).head(10)
    
    print("\n  🏆 Top 10 Movies (by average rating, min 10 ratings):")
    print("  " + "="*70)
    for idx, row in top_movies.iterrows():
        print(f"  {row['title'][:50]:50s} | ⭐ {row['mean_rating']:.2f} ({int(row['rating_count'])} ratings)")
    print("  " + "="*70)
    
    # Create visualization
    print("\n  📊 Creating visualization...")
    
    plt.figure(figsize=(12, 8))
    
    # Horizontal bar chart
    plt.barh(range(len(top_movies)), top_movies['mean_rating'], color='steelblue')
    plt.yticks(range(len(top_movies)), top_movies['title'])
    plt.xlabel('Average Rating', fontsize=12)
    plt.ylabel('Movie', fontsize=12)
    plt.title(f'Top 10 Movies by Average Rating\n(minimum {min_ratings} ratings)', 
              fontsize=14, fontweight='bold')
    plt.xlim(0, 5.5)  # MovieLens uses 1-5 scale
    
    # Add rating count annotations
    for i, (idx, row) in enumerate(top_movies.iterrows()):
        plt.text(row['mean_rating'] + 0.05, i, 
                f"{row['mean_rating']:.2f} ({int(row['rating_count'])})",
                va='center', fontsize=9)
    
    plt.grid(axis='x', alpha=0.3)
    plt.tight_layout()
    
    # Save plot
    out_path = os.path.join(OUT_DIR, 'top10_avg_rated_movies.png')
    plt.savefig(out_path, dpi=150, bbox_inches='tight')
    print(f"  ✅ Saved visualization to: {out_path}")
    
    # Additional statistics
    print("\n  📊 Dataset Statistics:")
    print(f"  • Total ratings: {len(df):,}")
    print(f"  • Unique users: {df['userId'].nunique():,}")
    print(f"  • Unique movies: {df['movieId'].nunique():,}")
    print(f"  • Average rating: {df['rating'].mean():.2f}")
    print(f"  • Rating distribution:")
    rating_dist = df['rating'].value_counts().sort_index()
    for rating, count in rating_dist.items():
        pct = (count / len(df)) * 100
        print(f"    ⭐ {rating}: {count:6,} ({pct:5.1f}%)")
    
    print("\n  ✅ Analysis complete!")
    
    return out_path


if __name__ == "__main__":
    # Test analysis function
    try:
        result_path = run_analysis()
        print(f"\n✅ Analysis successful! Plot saved to: {result_path}")
    except Exception as e:
        print(f"\n❌ Analysis failed: {str(e)}")
        import traceback
        traceback.print_exc()