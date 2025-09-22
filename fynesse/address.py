import geopandas as gpd
import pandas as pd
import numpy as np
import plotly.express as px
from sklearn.linear_model import LinearRegression
from ipywidgets import interact, IntSlider
import matplotlib.pyplot as plt
import seaborn as sns

# -------------------------------
# 1. Geospatial per-capita ranking
# -------------------------------
def rank_underserved_regions(facility_gdf, population_gdf, facility_col="hospitals"):
    merged = gpd.sjoin(
        facility_gdf, population_gdf, how="right", predicate="intersects"
    )
    region_counts = merged.groupby("region").size()
    per_capita = region_counts / population_gdf.groupby("region")["population"].sum()
    return per_capita.sort_values()

def suggest_priority_areas(per_capita_series, top_n=5):
    return per_capita_series.head(top_n)

# -------------------------------
# 2. Per-capita calculations & ranking
# -------------------------------
def get_underserved(df, top_n=5):
    df = df.copy()
    df["schools_per_10k"] = (df["schools"] / df["pop_total"]) * 10000
    df["hospitals_per_100k"] = (df["hospitals"] / df["pop_total"]) * 100000
    underserved_schools = df.nsmallest(top_n, "schools_per_10k")[["ADM1_EN", "schools_per_10k"]]
    underserved_hospitals = df.nsmallest(top_n, "hospitals_per_100k")[["ADM1_EN", "hospitals_per_100k"]]
    return underserved_schools, underserved_hospitals

# -------------------------------
# 3. Linear regression models
# -------------------------------
def fit_linear_models(df):
    X_s = df[["pop_total"]]
    y_s = df["schools"]
    model_s = LinearRegression().fit(X_s, y_s)
    
    X_h = df[["pop_total"]]
    y_h = df["hospitals"]
    model_h = LinearRegression().fit(X_h, y_h)
    
    return model_s, model_h

def predict_from_population(model_s, model_h, pop_val):
    pred_schools = model_s.predict([[pop_val]])[0]
    pred_hospitals = model_h.predict([[pop_val]])[0]
    return int(pred_schools), int(pred_hospitals)

# -------------------------------
# 4. Interactive notebook widget
# -------------------------------
def interactive_address(df):
    model_s, model_h = fit_linear_models(df)
    
    def view(pop_val):
        pred_schools, pred_hospitals = predict_from_population(model_s, model_h, pop_val)
        fig = px.scatter(
            df,
            x="pop_total",
            y="schools",
            size="hospitals",
            color="ADM1_EN",
            hover_name="ADM1_EN",
            hover_data={"pop_total": True, "schools": True, "hospitals": True},
            title=f"Population vs Schools vs Hospitals (Pop ~ {pop_val})",
            size_max=50
        )
        fig.show()
        print(f"Predicted schools for population {pop_val:,}: {pred_schools}")
        print(f"Predicted hospitals for population {pop_val:,}: {pred_hospitals}")
    
    interact(
        view,
        pop_val=IntSlider(
            min=int(df["pop_total"].min()),
            max=int(df["pop_total"].max()),
            step=10000,
            value=int(df["pop_total"].median()),
            description="Population"
        )
    )

# -------------------------------
# 5. Barplot visualization
# -------------------------------
def plot_per_capita(df):
    df = df.copy()
    df["schools_per_10k"] = (df["schools"] / df["pop_total"]) * 10000
    df["hospitals_per_100k"] = (df["hospitals"] / df["pop_total"]) * 100000
    
    plt.figure(figsize=(12,6))
    sns.barplot(data=df.sort_values("schools_per_10k", ascending=False),
                x="schools_per_10k", y="ADM1_EN", color="orange")
    plt.title("Schools per 10,000 People by County")
    plt.xlabel("Schools per 10,000")
    plt.ylabel("County")
    plt.show()
    
    plt.figure(figsize=(12,6))
    sns.barplot(data=df.sort_values("hospitals_per_100k", ascending=False),
                x="hospitals_per_100k", y="ADM1_EN", color="green")
    plt.title("Hospitals per 100,000 People by County")
    plt.xlabel("Hospitals per 100,000")
    plt.ylabel("County")
    plt.show()
