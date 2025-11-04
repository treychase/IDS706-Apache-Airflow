import pandas as pd
from sqlalchemy import create_engine, text
import os
import numpy as np
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend
import matplotlib.pyplot as plt
import sys

DB_CONN = os.getenv(
    'AIRFLOW__DATABASE__SQL_ALCHEMY_CONN',
    'postgresql+psycopg2://airflow:airflow@postgres:5432/airflow'
)

OUT_DIR = '/opt/airflow/data/analysis'
os.makedirs(OUT_DIR, exist_ok=True)


def haversine_distance(lat1, lon1, lat2, lon2):
    """
    Calculate great circle distance between two points on earth in kilometers.
    """
    try:
        # Convert to radians
        lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
        
        # Haversine formula
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
        c = 2 * np.arcsin(np.sqrt(a))
        
        # Radius of earth in kilometers
        return 6371 * c
    except Exception as e:
        print(f"  ⚠️  Error calculating distance: {str(e)}")
        return 0


def run_analysis():
    """
    Run simplified but comprehensive analysis on airport routes.
    Focus on reliability and clear output.
    """
    print("\n" + "="*80)
    print("📊 RUNNING ANALYSIS")
    print("="*80)
    
    table_name = 'airport_routes_merged'
    
    # Connect to database
    try:
        engine = create_engine(DB_CONN)
        print(f"  ✓ Connected to database")
    except Exception as e:
        raise ConnectionError(f"Failed to connect to database: {str(e)}")
    
    # Verify table exists
    try:
        with engine.connect() as conn:
            result = conn.execute(text(
                f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{table_name}')"
            )).fetchone()
            
            if not result[0]:
                raise ValueError(f"Table '{table_name}' does not exist in database!")
            print(f"  ✓ Table '{table_name}' exists")
    except Exception as e:
        raise ValueError(f"Failed to verify table: {str(e)}")
    
    # Read data from database
    try:
        print(f"  📖 Reading data from '{table_name}'...")
        df = pd.read_sql_table(table_name, engine)
        print(f"  ✓ Loaded {len(df):,} routes")
    except Exception as e:
        raise ValueError(f"Failed to read from database: {str(e)}")
    
    if len(df) == 0:
        raise ValueError("No data in table!")
    
    print(f"  ✓ Columns ({len(df.columns)}): {list(df.columns)[:8]}... (showing first 8)")
    
    # Calculate distances
    print(f"\n  📏 Calculating route distances...")
    try:
        df['distance_km'] = haversine_distance(
            df['source_latitude'],
            df['source_longitude'],
            df['dest_latitude'],
            df['dest_longitude']
        )
        print(f"  ✓ Distances calculated")
        print(f"     Mean: {df['distance_km'].mean():,.0f} km")
        print(f"     Max: {df['distance_km'].max():,.0f} km")
    except Exception as e:
        print(f"  ⚠️  Warning: Could not calculate distances: {str(e)}")
        df['distance_km'] = 0
    
    # Create visualizations
    print(f"\n  📊 Creating visualizations...")
    
    try:
        # Create figure with subplots
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('OpenFlights Route Analysis', fontsize=16, fontweight='bold')
        
        # Plot 1: Top Airlines by Route Count
        print(f"     Creating plot 1: Top airlines...")
        ax = axes[0, 0]
        top_airlines = df['airline'].value_counts().head(15)
        ax.barh(range(len(top_airlines)), top_airlines.values, color='steelblue')
        ax.set_yticks(range(len(top_airlines)))
        ax.set_yticklabels([name[:25] for name in top_airlines.index], fontsize=9)
        ax.set_xlabel('Number of Routes')
        ax.set_title('Top 15 Airlines by Route Count', fontsize=12, fontweight='bold')
        ax.grid(axis='x', alpha=0.3)
        
        # Plot 2: Routes by Country
        print(f"     Creating plot 2: Routes by country...")
        ax = axes[0, 1]
        top_countries = df['source_country'].value_counts().head(15)
        ax.barh(range(len(top_countries)), top_countries.values, color='coral')
        ax.set_yticks(range(len(top_countries)))
        ax.set_yticklabels([name[:25] for name in top_countries.index], fontsize=9)
        ax.set_xlabel('Number of Routes')
        ax.set_title('Top 15 Countries by Outgoing Routes', fontsize=12, fontweight='bold')
        ax.grid(axis='x', alpha=0.3)
        
        # Plot 3: Route Distance Distribution
        print(f"     Creating plot 3: Distance distribution...")
        ax = axes[1, 0]
        distances = df['distance_km'][df['distance_km'] > 0]
        if len(distances) > 0:
            ax.hist(distances, bins=50, color='green', edgecolor='black', alpha=0.7)
            ax.set_xlabel('Route Distance (km)')
            ax.set_ylabel('Number of Routes')
            ax.set_title('Route Distance Distribution', fontsize=12, fontweight='bold')
            ax.grid(axis='y', alpha=0.3)
            
            # Add mean and median lines
            mean_dist = distances.mean()
            median_dist = distances.median()
            ax.axvline(mean_dist, color='red', linestyle='--', linewidth=2, 
                      label=f'Mean: {mean_dist:.0f}km')
            ax.axvline(median_dist, color='blue', linestyle='--', linewidth=2, 
                      label=f'Median: {median_dist:.0f}km')
            ax.legend()
        else:
            ax.text(0.5, 0.5, 'No distance data available', 
                   ha='center', va='center', transform=ax.transAxes)
        
        # Plot 4: Busiest Airports
        print(f"     Creating plot 4: Busiest airports...")
        ax = axes[1, 1]
        airport_counts = df.groupby('source_airport_name').size().sort_values(ascending=False).head(15)
        ax.barh(range(len(airport_counts)), airport_counts.values, color='purple')
        ax.set_yticks(range(len(airport_counts)))
        ax.set_yticklabels([name[:30] for name in airport_counts.index], fontsize=9)
        ax.set_xlabel('Number of Outgoing Routes')
        ax.set_title('Top 15 Busiest Airports (by outgoing routes)', fontsize=12, fontweight='bold')
        ax.grid(axis='x', alpha=0.3)
        
        plt.tight_layout()
        
        # Save plot
        out_path = os.path.join(OUT_DIR, 'airport_analysis.png')
        plt.savefig(out_path, dpi=150, bbox_inches='tight')
        print(f"  ✅ Saved visualization to: {out_path}")
        plt.close()
        
    except Exception as e:
        print(f"  ⚠️  Warning: Visualization failed: {str(e)}")
        import traceback
        traceback.print_exc()
        # Don't fail the entire analysis if visualization fails
    
    # Print statistics
    print(f"\n  📊 SUMMARY STATISTICS")
    print("-"*80)
    print(f"  Total routes: {len(df):,}")
    print(f"  Unique airlines: {df['airline'].nunique():,}")
    print(f"  Unique source airports: {df['source_airport_id'].nunique():,}")
    print(f"  Unique destination airports: {df['dest_airport_id'].nunique():,}")
    print(f"  Unique countries (source): {df['source_country'].nunique():,}")
    print(f"  Direct flights (0 stops): {(df['stops'] == 0).sum():,} ({(df['stops'] == 0).sum()/len(df)*100:.1f}%)")
    
    if df['distance_km'].sum() > 0:
        print(f"\n  Distance Statistics:")
        print(f"    Mean: {df['distance_km'].mean():,.0f} km")
        print(f"    Median: {df['distance_km'].median():,.0f} km")
        print(f"    Min: {df['distance_km'].min():,.0f} km")
        print(f"    Max: {df['distance_km'].max():,.0f} km")
    
    print(f"\n  🏆 Top 5 Airlines:")
    for i, (airline, count) in enumerate(df['airline'].value_counts().head(5).items(), 1):
        print(f"    {i}. {airline[:40]:40s}: {count:>5,} routes")
    
    print(f"\n  🌍 Top 5 Countries:")
    for i, (country, count) in enumerate(df['source_country'].value_counts().head(5).items(), 1):
        print(f"    {i}. {country[:40]:40s}: {count:>5,} routes")
    
    print("\n✅ ANALYSIS COMPLETE!")
    print("="*80)
    
    return out_path


if __name__ == "__main__":
    try:
        result_path = run_analysis()
        print(f"\n✅ Analysis successful! Plot saved to: {result_path}")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Analysis failed: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)