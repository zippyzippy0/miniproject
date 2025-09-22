# fynesse/access.py

import os
import requests
import pandas as pd
import geopandas as gpd
import osmnx as ox


def load_local_csv(filepath: str) -> pd.DataFrame:
    """Load a local CSV file into a DataFrame."""
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"File not found: {filepath}")
    return pd.read_csv(filepath)


def load_local_shapefile(filepath: str) -> gpd.GeoDataFrame:
    """Load a local shapefile or GeoJSON into a GeoDataFrame."""
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"File not found: {filepath}")
    return gpd.read_file(filepath)


def load_osm_data(place_name: str, network_type: str = "all") -> gpd.GeoDataFrame:
    """
    Load OpenStreetMap data for a given place.
    
    Parameters:
        place_name (str): Location name (e.g., "Nairobi, Kenya").
        network_type (str): OSMnx network type ("all", "drive", "walk", "bike").
    """
    return ox.graph_from_place(place_name, network_type=network_type)


def load_from_github(raw_url: str, local_filename: str = None) -> str:
    """
    Download a file from GitHub (raw URL) and save it locally.
    
    Parameters:
        raw_url (str): The raw GitHub URL (must be raw, not blob).
        local_filename (str): Optional filename to save as. 
                              If None, uses last part of URL.
    
    Returns:
        str: Local file path.
    """
    if "github.com" in raw_url and "raw.githubusercontent.com" not in raw_url:
        # Convert "blob" URL to "raw"
        raw_url = raw_url.replace("github.com", "raw.githubusercontent.com").replace("/blob/", "/")
    
    if local_filename is None:
        local_filename = raw_url.split("/")[-1]
    
    if not os.path.exists(local_filename):
        r = requests.get(raw_url)
        if r.status_code != 200:
            raise ConnectionError(f"Failed to download file: {raw_url}")
        with open(local_filename, "wb") as f:
            f.write(r.content)
    
    return local_filename
