# assess.py
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
from scipy.stats import pearsonr, norm

# -------------------------------
# Data Merging & Preparation
# -------------------------------
def merge_facilities(pop_counts, counts_schools, counts_health):
    """
    Merge population counts with school and health facility counts per county.
    Returns a combined DataFrame with all columns needed for Assess stage.
    """
    common_counties = set(pop_counts["ADM1_EN"]).intersection(counts_schools["ADM1_EN"], counts_health["ADM1_EN"])
    df = (
        pop_counts[pop_counts["ADM1_EN"].isin(common_counties)]
        .merge(counts_schools, on="ADM1_EN", how="inner")
        .merge(counts_health, on="ADM1_EN", how="inner")
        .fillna(0)
    )
    df.rename(columns={"count_s": "schools", "counts": "hospitals"}, inplace=True)
    return df

# -------------------------------
# Correlation Analysis
# -------------------------------
def plot_correlation(df, cols, title="Correlation Heatmap"):
    corr = df[cols].corr()
    plt.figure(figsize=(6,5))
    sns.heatmap(corr, annot=True, cmap="coolwarm", fmt=".2f")
    plt.title(title)
    plt.show()
    return corr

def pearson_correlation(df, col1, col2):
    corr, pval = pearsonr(df[col1], df[col2])
    return corr, pval

# -------------------------------
# Normalized Comparison Plots
# -------------------------------
def plot_normalized_stacked(df, col_pop="pop_total", col_schools="schools"):
    df_norm = df.copy()
    df_norm["pop_total_norm"] = df_norm[col_pop] / df_norm[col_pop].sum() * 100
    df_norm["schools_norm"] = df_norm[col_schools] / df_norm[col_schools].sum() * 100
    df_sorted = df_norm.sort_values("pop_total_norm")

    fig = px.bar(
        df_sorted,
        y="ADM1_EN",
        x=["pop_total_norm", "schools_norm"],
        orientation='h',
        barmode='stack',
        labels={"value":"Percentage Share (%)", "ADM1_EN":"County"},
        height=2000
    )
    fig.update_layout(title="Population vs Schools per County (Stacked Normalized %)")
    fig.show()

# -------------------------------
# Distance Analysis
# -------------------------------
def compute_distances(counties_projected, facilities_projected, df_county):
    """
    Compute distances from county centroids to nearest facility.
    Returns df_county with new column 'dist_to_nearest_facility_km'.
    """
    from shapely.ops import nearest_points

    distances_km = []
    for index, row in df_county.iterrows():
        county_name = row["ADM1_EN"]
        county_geom = counties_projected[counties_projected["ADM1_EN"] == county_name].geometry.iloc[0]
        county_center = county_geom.centroid

        nearest_distance = facilities_projected.distance(county_center).min()
        distances_km.append(nearest_distance / 1000.0)

    df_county["dist_to_nearest_facility_km"] = distances_km
    return df_county

def plot_distance_distribution(df_county, column="dist_to_nearest_facility_km"):
    mu, std = norm.fit(df_county[column])
    plt.figure(figsize=(10,5))
    sns.histplot(df_county[column], kde=True, color="green")
    plt.axvline(mu, color="red", linestyle="--")
    plt.title(f"Distances to Nearest Facility (mean={mu:.2f} km)")
    plt.xlabel("Distance (km)")
    plt.ylabel("Frequency")
    plt.show()
    return mu, std

# -------------------------------
# Bernoulli Access Probability
# -------------------------------
def bernoulli_access(df_county, threshold_km=5, column="dist_to_nearest_facility_km"):
    bern_outcome = (df_county[column] <= threshold_km).astype(int)
    prob_access = bern_outcome.mean()
    return prob_access
