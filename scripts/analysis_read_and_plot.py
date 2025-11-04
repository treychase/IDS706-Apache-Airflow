import pandas as pd
from sqlalchemy import create_engine
import os
import numpy as np
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend for server environments
import matplotlib.pyplot as plt
import seaborn as sns

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


def assign_continent(lat, lon):
    """
    Simple continent assignment based on latitude/longitude.
    """
    if lat > 35 and lon > -10 and lon < 50:
        return 'Europe'
    elif lat > 35 and lon > 50 and lon < 150:
        return 'Asia'
    elif lat < 35 and lat > -35 and lon > 50 and lon < 150:
        return 'Asia/Oceania'
    elif lat < -10 and lon > 110 and lon < 180:
        return 'Oceania'
    elif lat > 15 and lon < -30:
        return 'North America'
    elif lat < 15 and lat > -60 and lon < -30:
        return 'South America'
    elif lat > -35 and lat < 35 and lon > -20 and lon < 55:
        return 'Africa'
    else:
        return 'Other'


def run_analysis():
    """
    Run comprehensive analysis on airport routes:
    1. Geographic analysis by continent
    2. Airline analysis and market share
    3. Distance analysis by country
    """
    print("📊 Running comprehensive analysis...")
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
    
    # Assign continents based on coordinates
    print("  🌍 Assigning continents...")
    df['source_continent'] = df.apply(
        lambda row: assign_continent(row['source_latitude'], row['source_longitude']), 
        axis=1
    )
    df['dest_continent'] = df.apply(
        lambda row: assign_continent(row['dest_latitude'], row['dest_longitude']), 
        axis=1
    )
    
    # Create figure with multiple subplots
    fig = plt.figure(figsize=(20, 12))
    gs = fig.add_gridspec(3, 3, hspace=0.3, wspace=0.3)
    
    # =========================================================================
    # ANALYSIS 1: GEOGRAPHIC ANALYSIS
    # =========================================================================
    print("\n  🌍 GEOGRAPHIC ANALYSIS")
    print("  " + "="*80)
    
    # 1.1: Routes by continent
    ax1 = fig.add_subplot(gs[0, 0])
    continent_routes = df['source_continent'].value_counts()
    ax1.bar(range(len(continent_routes)), continent_routes.values, color='steelblue')
    ax1.set_xticks(range(len(continent_routes)))
    ax1.set_xticklabels(continent_routes.index, rotation=45, ha='right')
    ax1.set_title('Routes by Source Continent', fontsize=12, fontweight='bold')
    ax1.set_ylabel('Number of Routes')
    ax1.grid(axis='y', alpha=0.3)
    
    print(f"\n  📊 Routes by Continent:")
    for continent, count in continent_routes.items():
        print(f"    {continent:20s}: {count:>6,} routes")
    
    # 1.2: Longest route per continent
    ax2 = fig.add_subplot(gs[0, 1])
    longest_per_continent = df.groupby('source_continent').agg({
        'distance_km': 'max'
    }).sort_values('distance_km', ascending=False)
    
    ax2.barh(range(len(longest_per_continent)), longest_per_continent['distance_km'], color='coral')
    ax2.set_yticks(range(len(longest_per_continent)))
    ax2.set_yticklabels(longest_per_continent.index)
    ax2.set_xlabel('Distance (km)')
    ax2.set_title('Longest Route by Continent', fontsize=12, fontweight='bold')
    ax2.grid(axis='x', alpha=0.3)
    
    print(f"\n  🛫 Longest Route per Continent:")
    for continent, row in longest_per_continent.iterrows():
        print(f"    {continent:20s}: {row['distance_km']:>8,.0f} km")
    
    # 1.3: Inter-continental vs intra-continental routes
    ax3 = fig.add_subplot(gs[0, 2])
    df['route_type'] = df.apply(
        lambda row: 'Domestic' if row['source_country'] == row['dest_country'] 
        else ('Intra-continental' if row['source_continent'] == row['dest_continent'] 
              else 'Inter-continental'),
        axis=1
    )
    route_types = df['route_type'].value_counts()
    colors = ['#ff9999', '#66b3ff', '#99ff99']
    ax3.pie(route_types.values, labels=route_types.index, autopct='%1.1f%%', 
            colors=colors, startangle=90)
    ax3.set_title('Route Types Distribution', fontsize=12, fontweight='bold')
    
    print(f"\n  ✈️  Route Types:")
    for route_type, count in route_types.items():
        pct = (count / len(df)) * 100
        print(f"    {route_type:20s}: {count:>6,} ({pct:>5.1f}%)")
    
    # =========================================================================
    # ANALYSIS 2: AIRLINE ANALYSIS
    # =========================================================================
    print("\n  ✈️  AIRLINE ANALYSIS")
    print("  " + "="*80)
    
    # 2.1: Top airlines by route count
    ax4 = fig.add_subplot(gs[1, 0])
    top_airlines = df['airline'].value_counts().head(10)
    ax4.barh(range(len(top_airlines)), top_airlines.values, color='green')
    ax4.set_yticks(range(len(top_airlines)))
    ax4.set_yticklabels([name[:20] for name in top_airlines.index], fontsize=9)
    ax4.set_xlabel('Number of Routes')
    ax4.set_title('Top 10 Airlines by Route Count', fontsize=12, fontweight='bold')
    ax4.grid(axis='x', alpha=0.3)
    
    # Add route count labels
    for i, (airline, count) in enumerate(top_airlines.items()):
        ax4.text(count + 20, i, f'{count:,}', va='center', fontsize=9)
    
    print(f"\n  🏆 Top 10 Airlines by Route Count:")
    for i, (airline, count) in enumerate(top_airlines.items(), 1):
        print(f"    {i:2d}. {airline[:30]:30s}: {count:>5,} routes")
    
    # 2.2: Market share of top airlines
    ax5 = fig.add_subplot(gs[1, 1])
    market_share = df['airline'].value_counts().head(8)
    others = len(df) - market_share.sum()
    market_share = pd.concat([market_share, pd.Series({'Others': others})])
    
    colors_palette = plt.cm.Set3(range(len(market_share)))
    ax5.pie(market_share.values, labels=[name[:15] for name in market_share.index], 
            autopct='%1.1f%%', colors=colors_palette, startangle=90)
    ax5.set_title('Market Share - Top Airlines', fontsize=12, fontweight='bold')
    
    print(f"\n  📊 Market Share (Top 8 Airlines):")
    total = len(df)
    for airline, count in market_share.items():
        pct = (count / total) * 100
        print(f"    {airline[:30]:30s}: {pct:>5.1f}%")
    
    # 2.3: Average route distance by top airlines
    ax6 = fig.add_subplot(gs[1, 2])
    airline_avg_dist = df.groupby('airline').agg({
        'distance_km': 'mean',
        'airline': 'count'
    }).rename(columns={'airline': 'route_count'})
    
    # Filter airlines with at least 50 routes
    airline_avg_dist = airline_avg_dist[airline_avg_dist['route_count'] >= 50]
    airline_avg_dist = airline_avg_dist.sort_values('distance_km', ascending=False).head(10)
    
    ax6.barh(range(len(airline_avg_dist)), airline_avg_dist['distance_km'], color='purple')
    ax6.set_yticks(range(len(airline_avg_dist)))
    ax6.set_yticklabels([name[:20] for name in airline_avg_dist.index], fontsize=9)
    ax6.set_xlabel('Average Distance (km)')
    ax6.set_title('Avg Route Distance - Top Airlines (50+ routes)', fontsize=12, fontweight='bold')
    ax6.grid(axis='x', alpha=0.3)
    
    print(f"\n  📏 Airlines with Longest Average Routes (50+ routes):")
    for i, (airline, row) in enumerate(airline_avg_dist.iterrows(), 1):
        print(f"    {i:2d}. {airline[:30]:30s}: {row['distance_km']:>7,.0f} km avg ({int(row['route_count'])} routes)")
    
    # =========================================================================
    # ANALYSIS 3: DISTANCE ANALYSIS BY COUNTRY
    # =========================================================================
    print("\n  📏 DISTANCE ANALYSIS")
    print("  " + "="*80)
    
    # 3.1: Average distance by source country (top 15)
    ax7 = fig.add_subplot(gs[2, 0])
    country_dist = df.groupby('source_country').agg({
        'distance_km': 'mean',
        'source_country': 'count'
    }).rename(columns={'source_country': 'route_count'})
    
    # Filter countries with at least 100 routes
    country_dist = country_dist[country_dist['route_count'] >= 100]
    country_dist = country_dist.sort_values('distance_km', ascending=False).head(15)
    
    ax7.barh(range(len(country_dist)), country_dist['distance_km'], color='orange')
    ax7.set_yticks(range(len(country_dist)))
    ax7.set_yticklabels([name[:20] for name in country_dist.index], fontsize=9)
    ax7.set_xlabel('Average Distance (km)')
    ax7.set_title('Avg Route Distance by Country (100+ routes)', fontsize=12, fontweight='bold')
    ax7.grid(axis='x', alpha=0.3)
    
    print(f"\n  🌍 Countries with Longest Average Routes (100+ routes):")
    for i, (country, row) in enumerate(country_dist.iterrows(), 1):
        print(f"    {i:2d}. {country[:30]:30s}: {row['distance_km']:>7,.0f} km avg")
    
    # 3.2: Distance distribution with outliers
    ax8 = fig.add_subplot(gs[2, 1])
    ax8.hist(df['distance_km'], bins=50, color='teal', edgecolor='black', alpha=0.7)
    ax8.set_xlabel('Route Distance (km)')
    ax8.set_ylabel('Number of Routes')
    ax8.set_title('Route Distance Distribution', fontsize=12, fontweight='bold')
    ax8.grid(axis='y', alpha=0.3)
    
    # Add statistics
    mean_dist = df['distance_km'].mean()
    median_dist = df['distance_km'].median()
    ax8.axvline(mean_dist, color='red', linestyle='--', linewidth=2, 
                label=f'Mean: {mean_dist:.0f}km')
    ax8.axvline(median_dist, color='green', linestyle='--', linewidth=2, 
                label=f'Median: {median_dist:.0f}km')
    ax8.legend()
    
    print(f"\n  📊 Distance Statistics:")
    print(f"    Mean distance    : {df['distance_km'].mean():>8,.0f} km")
    print(f"    Median distance  : {df['distance_km'].median():>8,.0f} km")
    print(f"    Std deviation    : {df['distance_km'].std():>8,.0f} km")
    print(f"    Min distance     : {df['distance_km'].min():>8,.0f} km")
    print(f"    Max distance     : {df['distance_km'].max():>8,.0f} km")
    
    # 3.3: Top 10 longest routes
    ax9 = fig.add_subplot(gs[2, 2])
    longest_routes = df.nlargest(10, 'distance_km')[
        ['source_city', 'source_country', 'dest_city', 'dest_country', 'distance_km', 'airline']
    ]
    
    # Create route labels
    route_labels = []
    for _, row in longest_routes.iterrows():
        label = f"{row['source_city'][:12]} → {row['dest_city'][:12]}"
        route_labels.append(label)
    
    ax9.barh(range(len(longest_routes)), longest_routes['distance_km'], color='crimson')
    ax9.set_yticks(range(len(longest_routes)))
    ax9.set_yticklabels(route_labels, fontsize=8)
    ax9.set_xlabel('Distance (km)')
    ax9.set_title('Top 10 Longest Routes', fontsize=12, fontweight='bold')
    ax9.grid(axis='x', alpha=0.3)
    
    print(f"\n  🌍 Top 10 Longest Routes:")
    for i, (_, row) in enumerate(longest_routes.iterrows(), 1):
        print(f"    {i:2d}. {row['source_city']:15s} → {row['dest_city']:15s}")
        print(f"        ({row['source_country']} → {row['dest_country']})")
        print(f"        Distance: {row['distance_km']:,.0f} km | Airline: {row['airline']}")
    
    # Save the comprehensive plot
    plt.suptitle('OpenFlights Comprehensive Analysis\nGeographic, Airline & Distance Analysis', 
                 fontsize=16, fontweight='bold', y=0.995)
    
    out_path = os.path.join(OUT_DIR, 'airport_analysis.png')
    plt.savefig(out_path, dpi=150, bbox_inches='tight')
    print(f"\n  ✅ Saved comprehensive visualization to: {out_path}")
    
    # =========================================================================
    # ADDITIONAL INSIGHTS
    # =========================================================================
    print("\n  💡 ADDITIONAL INSIGHTS")
    print("  " + "="*80)
    
    # Dataset summary
    print(f"\n  📊 Dataset Summary:")
    print(f"    Total routes              : {len(df):,}")
    print(f"    Unique source airports    : {df['source_airport_id'].nunique():,}")
    print(f"    Unique destination airports: {df['dest_airport_id'].nunique():,}")
    print(f"    Unique airlines           : {df['airline'].nunique():,}")
    print(f"    Unique countries (source) : {df['source_country'].nunique():,}")
    print(f"    Direct flights (0 stops)  : {(df['stops'] == 0).sum():,} ({(df['stops'] == 0).sum()/len(df)*100:.1f}%)")
    
    # Busiest airports
    print(f"\n  🏆 Top 5 Busiest Airports (by outgoing routes):")
    busiest = df.groupby(['source_airport_name', 'source_city', 'source_country']).size().reset_index(name='routes')
    busiest = busiest.sort_values('routes', ascending=False).head(5)
    for i, row in busiest.iterrows():
        print(f"    {row['source_airport_name'][:30]:30s} ({row['source_city']}, {row['source_country']}): {row['routes']:,} routes")
    
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