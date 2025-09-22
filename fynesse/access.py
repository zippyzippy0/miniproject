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


def load_shapefile_from_github(base_url: str, prefix: str) -> str:
    """
    Download all necessary shapefile components (.shp, .shx, .dbf, .prj) 
    from a GitHub raw base URL into local data directory.
    
    Args:
        base_url (str): Base raw GitHub URL without trailing slash
        prefix (str): Common file prefix (e.g., 'ken_admbnda_adm0_iebc_20191031')
    
    Returns:
        str: Path to the local .shp file
    """
    exts = ["shp", "shx", "dbf", "prj"]
    local_files = []
    for ext in exts:
        url = f"{base_url}/{prefix}.{ext}"
        local_path = os.path.join(DATA_DIR, f"{prefix}.{ext}")
        if not os.path.exists(local_path):
            r = requests.get(url)
            if r.status_code == 200:
                os.makedirs(DATA_DIR, exist_ok=True)
                with open(local_path, "wb") as f:
                    f.write(r.content)
            else:
                raise ConnectionError(f"Failed to download {url}")
        local_files.append(local_path)
    return local_files[0]  # return path to .shp


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
