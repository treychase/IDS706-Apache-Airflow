import pandas as pd
from sqlalchemy import create_engine
import os
import numpy as np
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend for server environments
import matplotlib.pyplot as plt

DB_CONN = os.getenv(
    'AIRFLOW__DATABASE__SQL_ALCHEMY_CONN',
    'postgresql+psycopg2://airflow:airflow@postgres:5432/airflow'
)

OUT_DIR = '/opt/airflow/data/analysis'
os.makedirs(OUT_DIR, exist_ok=True)


def haversine_distance(lat1, lon1, lat2, lon2):
    """
    Calculate the great circle distance between two points 
    on the earth (specified in decimal degrees).
    Returns distance in kilometers.
    """
    # Convert decimal degrees to radians
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    
    # Haversine formula
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
    c = 2 * np.arcsin(np.sqrt(a))
    
    # Radius of earth in kilometers
    r = 6371
    
    return c * r


def run_analysis():
    """
    Read data from PostgreSQL and perform analysis on airport routes.
    Creates visualization of busiest airports by route count.
    """
    print("📊 Running analysis...")
    print(f"  Database: {DB_CONN.split('@')[1] if '@' in DB_CONN else 'unknown'}")
    
    # Connect to database
    engine = create_engine(DB_CONN)
    
    # Read merged data from database
    table_name = 'airport_routes_merged'
    print(f"  📖 Reading from table: {table_name}")
    
    df = pd.read_sql_table(table_name, engine)
    print(f"  ✓ Loaded {len(df)} routes from database")
    print(f"  ✓ Columns: {list(df.columns)}")
    
    # Calculate route distances using Haversine formula
    print("  📏 Calculating route distances...")
    df['distance_km'] = haversine_distance(
        df['source_latitude'], 
        df['source_longitude'],
        df['dest_latitude'], 
        df['dest_longitude']
    )
    
    # Analysis 1: Busiest airports by number of outgoing routes
    print("  📈 Analyzing busiest airports...")
    
    # Count routes from each airport
    airport_routes = df.groupby(['source_airport_id', 'source_airport_name', 'source_city', 'source_country']).agg({
        'dest_airport_id': 'count',
        'distance_km': ['mean', 'max']
    }).reset_index()
    
    # Flatten column names
    airport_routes.columns = [
        'airport_id', 'airport_name', 'city', 'country', 
        'route_count', 'avg_distance_km', 'max_distance_km'
    ]
    
    # Get top 10 busiest airports
    top_airports = airport_routes.nlargest(10, 'route_count')
    
    print("\n  🏆 Top 10 Busiest Airports (by number of outgoing routes):")
    print("  " + "="*80)
    for idx, row in top_airports.iterrows():
        print(f"  {row['airport_name'][:35]:35s} ({row['city']}, {row['country']}) | "
              f"🛫 {int(row['route_count'])} routes | "
              f"📏 Avg: {row['avg_distance_km']:.0f}km")
    print("  " + "="*80)
    
    # Create visualization
    print("\n  📊 Creating busiest airports visualization...")
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 8))
    
    # Chart 1: Top 10 Busiest Airports by Route Count
    ax1.barh(range(len(top_airports)), top_airports['route_count'], color='steelblue')
    ax1.set_yticks(range(len(top_airports)))
    
    # Create labels with city and country
    labels = [f"{row['airport_name'][:25]}\n({row['city']}, {row['country']})" 
              for _, row in top_airports.iterrows()]
    ax1.set_yticklabels(labels, fontsize=9)
    
    ax1.set_xlabel('Number of Outgoing Routes', fontsize=11, fontweight='bold')
    ax1.set_ylabel('Airport', fontsize=11, fontweight='bold')
    ax1.set_title('Top 10 Busiest Airports by Route Count', fontsize=13, fontweight='bold')
    ax1.grid(axis='x', alpha=0.3)
    
    # Add count annotations
    for i, (idx, row) in enumerate(top_airports.iterrows()):
        ax1.text(row['route_count'] + 5, i, f"{int(row['route_count'])}",
                va='center', fontsize=10, fontweight='bold')
    
    # Chart 2: Distribution of Route Distances
    print("  📊 Creating route distance distribution...")
    
    ax2.hist(df['distance_km'], bins=50, color='coral', edgecolor='black', alpha=0.7)
    ax2.set_xlabel('Route Distance (km)', fontsize=11, fontweight='bold')
    ax2.set_ylabel('Number of Routes', fontsize=11, fontweight='bold')
    ax2.set_title('Distribution of Route Distances', fontsize=13, fontweight='bold')
    ax2.grid(axis='y', alpha=0.3)
    
    # Add statistics to the plot
    mean_dist = df['distance_km'].mean()
    median_dist = df['distance_km'].median()
    ax2.axvline(mean_dist, color='red', linestyle='--', linewidth=2, label=f'Mean: {mean_dist:.0f}km')
    ax2.axvline(median_dist, color='green', linestyle='--', linewidth=2, label=f'Median: {median_dist:.0f}km')
    ax2.legend()
    
    plt.tight_layout()
    
    # Save plot
    out_path = os.path.join(OUT_DIR, 'airport_analysis.png')
    plt.savefig(out_path, dpi=150, bbox_inches='tight')
    print(f"  ✅ Saved visualization to: {out_path}")
    
    # Additional statistics
    print("\n  📊 Dataset Statistics:")
    print(f"  • Total routes: {len(df):,}")
    print(f"  • Unique source airports: {df['source_airport_id'].nunique():,}")
    print(f"  • Unique destination airports: {df['dest_airport_id'].nunique():,}")
    print(f"  • Unique airlines: {df['airline'].nunique():,}")
    print(f"  • Direct flights (0 stops): {(df['stops'] == 0).sum():,} ({(df['stops'] == 0).sum()/len(df)*100:.1f}%)")
    print(f"\n  📏 Distance Statistics:")
    print(f"  • Average route distance: {df['distance_km'].mean():.0f} km")
    print(f"  • Median route distance: {df['distance_km'].median():.0f} km")
    print(f"  • Shortest route: {df['distance_km'].min():.0f} km")
    print(f"  • Longest route: {df['distance_km'].max():.0f} km")
    
    # Find the longest route
    longest = df.nlargest(1, 'distance_km').iloc[0]
    print(f"\n  🌍 Longest Route:")
    print(f"  • {longest['source_airport_name']} ({longest['source_city']}, {longest['source_country']})")
    print(f"    → {longest['dest_airport_name']} ({longest['dest_city']}, {longest['dest_country']})")
    print(f"  • Distance: {longest['distance_km']:.0f} km")
    print(f"  • Airline: {longest['airline']}")
    
    # Top airlines by route count
    print(f"\n  ✈️  Top 5 Airlines by Route Count:")
    top_airlines = df['airline'].value_counts().head(5)
    for airline, count in top_airlines.items():
        print(f"  • {airline}: {count:,} routes")
    
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