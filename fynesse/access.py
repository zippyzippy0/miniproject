import os
import requests
import warnings
import pandas as pd
import geopandas as gpd
import osmnx as ox

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.resource import Resource

# Common data directory (always relative to project structure)
DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")

def _resolve_path(filename: str) -> str:
    """Return absolute path inside the data directory."""
    return os.path.join(DATA_DIR, filename)


# -----------------------------
# Local data loaders
# -----------------------------

def load_local_csv(filename: str) -> pd.DataFrame:
    path = _resolve_path(filename)
    if not os.path.exists(path):
        raise FileNotFoundError(f"File not found: {path}")
    return pd.read_csv(path)

def load_local_excel(filename: str, sheet_name: str = None) -> pd.DataFrame:
    path = _resolve_path(filename)
    if not os.path.exists(path):
        raise FileNotFoundError(f"File not found: {path}")
    return pd.read_excel(path, sheet_name=sheet_name)

def load_local_json(filename: str) -> pd.DataFrame:
    path = _resolve_path(filename)
    if not os.path.exists(path):
        raise FileNotFoundError(f"File not found: {path}")
    return pd.read_json(path)

def load_local_shapefile(filename: str) -> gpd.GeoDataFrame:
    path = _resolve_path(filename)
    if not os.path.exists(path):
        raise FileNotFoundError(f"File not found: {path}")
    return gpd.read_file(path)


# -----------------------------
# Download & API utilities
# -----------------------------

def download_file(url: str, save_as: str) -> str:
    save_path = _resolve_path(save_as)
    response = requests.get(url)
    if response.status_code == 200:
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        with open(save_path, "wb") as f:
            f.write(response.content)
        return save_path
    else:
        raise Exception(f"Failed to download {url}, status code {response.status_code}")

def fetch_api_json(url: str, params: dict = None) -> pd.DataFrame:
    response = requests.get(url, params=params)
    if response.status_code == 200:
        return pd.json_normalize(response.json())
    else:
        raise Exception(f"Failed API request {url}, status code {response.status_code}")


# -----------------------------
# OSM data utilities
# -----------------------------

def load_osm_data(place: str, tags: dict):
    try:
        gdf = ox.geometries_from_place(place, tags)
    except AttributeError:
        gdf = ox.pois_from_place(place, tags)
    return gdf


# -----------------------------
# HDX data utilities
# -----------------------------

def init_hdx():
    Configuration.create(
        hdx_site="prod", user_agent="fynesse", hdx_read_only=True
    )

def search_hdx_datasets(query: str):
    return Dataset.search_in_hdx(query)

def download_hdx_resource(dataset_name: str, resource_name: str, save_as: str) -> str:
    save_path = _resolve_path(save_as)
    dataset = Dataset.read_from_hdx(dataset_name)
    for resource in dataset.get_resources():
        if resource_name.lower() in resource["name"].lower():
            return resource.download(save_path)
    raise ValueError(f"Resource {resource_name} not found in dataset {dataset_name}")
