import os
import requests
import pandas as pd
import geopandas as gpd
import osmnx as ox
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset

DATA_DIR = os.path.join("..", "data")
os.makedirs(DATA_DIR, exist_ok=True)

def load_file(filepath, filetype=None, sheet_name=None):
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"File not found: {filepath}")
    if filetype is None:
        ext = os.path.splitext(filepath)[1].lower()
        filetype = {"csv":"csv", ".xlsx":"excel", ".xls":"excel",
                    ".json":"json", ".shp":"shp"}.get(ext, None)
        if filetype is None:
            raise ValueError(f"Cannot infer file type from extension '{ext}'")
    if filetype == "csv":
        return pd.read_csv(filepath)
    elif filetype == "excel":
        return pd.read_excel(filepath, sheet_name=sheet_name)
    elif filetype == "json":
        return pd.read_json(filepath)
    elif filetype == "shp":
        return gpd.read_file(filepath)
    else:
        raise ValueError(f"Unsupported file type: {filetype}")

def download_file(url, save_path=None):
    save_path = save_path or os.path.join(DATA_DIR, os.path.basename(url))
    if not os.path.exists(save_path):
        r = requests.get(url)
        if r.status_code != 200:
            raise ConnectionError(f"Failed to download {url}, status {r.status_code}")
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        with open(save_path, "wb") as f:
            f.write(r.content)
    return save_path

def load_osm(place, tags):
    try:
        return ox.geometries_from_place(place, tags)
    except AttributeError:
        return ox.pois_from_place(place, tags)

def init_hdx(read_only=True):
    Configuration.create(hdx_site="prod", user_agent="notebook", hdx_read_only=read_only)

def search_hdx(query):
    return Dataset.search_in_hdx(query)

def download_hdx_resource(dataset_name, resource_name, save_path=None):
    dataset = Dataset.read_from_hdx(dataset_name)
    for resource in dataset.get_resources():
        if resource_name.lower() in resource["name"].lower():
            return resource.download(save_path or DATA_DIR)
    raise ValueError(f"Resource '{resource_name}' not found in dataset '{dataset_name}'")

def load_shapefile_from_github(base_url, prefix):
    exts = ["shp", "shx", "dbf", "prj"]
    local_paths = []
    for ext in exts:
        url = f"{base_url}/{prefix}.{ext}"
        local_path = os.path.join(DATA_DIR, f"{prefix}.{ext}")
        download_file(url, local_path)
        local_paths.append(local_path)
    return local_paths[0]
