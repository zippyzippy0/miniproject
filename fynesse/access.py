# fynesse/access.py
import os
import pandas as pd
import requests
import osmnx as ox
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



def init_hdx(user_agent: str = "fynesse_project"):
    Configuration.create(hdx_site="prod", user_agent=user_agent)


def search_hdx_datasets(query: str):
    results = Dataset.search_in_hdx(query)
    return results


def download_hdx_resource(dataset_name: str, resource_name: str, save_path: str) -> str:
    dataset = Dataset.read_from_hdx(dataset_name)
    for resource in dataset.get_resources():
        if resource_name.lower() in resource["name"].lower():
            path = resource.download(save_path)
            return path
    raise ValueError(f"Resource {resource_name} not found in dataset {dataset_name}")


def fetch_api_json(url: str, params: dict = None) -> pd.DataFrame:
    response = requests.get(url, params=params)
    if response.status_code == 200:
        return pd.json_normalize(response.json())
    else:
        raise Exception(f"Failed API request {url}, status code {response.status_code}")
