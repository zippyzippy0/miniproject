import geopandas as gpd


def rank_underserved_regions(facility_gdf, population_gdf):
    merged = gpd.sjoin(
        facility_gdf, population_gdf, how="right", predicate="intersects"
    )
    region_counts = merged.groupby("region").size()
    per_capita = region_counts / population_gdf.groupby("region")["population"].sum()
    return per_capita.sort_values()


def suggest_priority_areas(per_capita_series, top_n=5):
    return per_capita_series.head(top_n)
