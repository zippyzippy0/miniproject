import os
import requests
import warnings
import osmnx as ox
import pandas as pd

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.resource import Resource


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
