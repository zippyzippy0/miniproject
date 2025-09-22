import os
import requests
import warnings
import osmnx as ox
import pandas as pd
import geopandas as gpd

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")

# -------------------------------
# Local file loaders
# -------------------------------
def load_local_csv(filepath: str) -> pd.DataFrame:
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"File not found: {filepath}")
    return pd.read_csv(filepath)

def load_local_excel(filepath: str, sheet_name: str = None) -> pd.DataFrame:
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"File not found: {filepath}")
    return pd.read_excel(filepath, sheet_name=sheet_name)

def load_local_json(filepath: str) -> pd.DataFrame:
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"File not found: {filepath}")
    return pd.read_json(filepath)

def load_local_shapefile(filepath: str) -> gpd.GeoDataFrame:
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"File not found: {filepath}")
    return gpd.read_file(filepath)

# -------------------------------
# Remote loaders
# -------------------------------
def download_file(url: str, save_path: str) -> str:
    response = requests.get(url)
    if response.status_code == 200:
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        with open(save_path, "wb") as f:
            f.write(response.content)
        return save_path
    else:
        raise Exception(f"Failed to download {url}, status code {response.status_code}")

def load_osm_data(place: str, tags: dict):
    try:
        gdf = ox.geometries_from_place(place, tags)
    except AttributeError:
        gdf = ox.pois_from_place(place, tags)
    return gdf

# -------------------------------
# HDX helpers
# -------------------------------
def init_hdx():
    Configuration.create(
        hdx_site="prod", user_agent="fynesse", hdx_read_only=True
    )

def search_hdx_datasets(query: str):
    return Dataset.search_in_hdx(query)

def download_hdx_resource(dataset_name: str, resource_name: str, save_path: str) -> str:
    dataset = Dataset.read_from_hdx(dataset_name)
    for resource in dataset.get_resources():
        if resource_name.lower() in resource["name"].lower():
            return resource.download(save_path)
    raise ValueError(f"Resource {resource_name} not found in dataset {dataset_name}")

# -------------------------------
# GitHub shapefile helper
# -------------------------------
def load_shapefile_from_github(base_url: str, prefix: str) -> str:
    """
    Download all necessary shapefile components (.shp, .shx, .dbf, .prj)
    from a GitHub raw base URL into local data directory.

    Args:
        base_url (str): Base raw GitHub URL (without trailing slash)
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
