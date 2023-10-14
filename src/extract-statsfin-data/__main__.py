import json
import requests
import csv
import re
import logging
import tomllib
import os
import pandas as pd


logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

def load_config():
    with open(os.path.join(os.path.dirname(__file__), "config.toml"), mode="rb") as file:
        config = tomllib.load(file)
    return config

def extract_postal_code_mapping(url):
    with open(os.path.join(os.path.dirname(__file__), "queries", "postal_codes_query.json"), "r") as file:
        query = json.load(file)
    response = requests.post(url=url, json=query)
    if response.status_code != 200:
        logging.error(f"Request to Postal code API server failed with status code: {response.status_code}")
        raise Exception("No response from API")
    content = response.text
    postal_codes_combined = [re.split("\s+", line[0], 1) for line in list(csv.reader(content.splitlines(), delimiter=','))[2:]]
    postal_code_mapping = [[pair[0], pair[1].split("(")[0].strip(),  pair[1].split("(")[1].replace(")", "")] for pair in postal_codes_combined]
    df = pd.DataFrame(postal_code_mapping, columns=["code", "name", "municipality"])
    filtered_df = df.loc[df["municipality"].isin(["Helsinki", "Espoo", "Tampere", "Oulu", "Vantaa", "Turku"]),:]
    return filtered_df

def extract_postal_code_info(url, postal_codes):
    with open(os.path.join(os.path.dirname(__file__), "queries", "postal_area_basics_query.json"), "r") as file:
        query = json.load(file)
    query["query"][0]["selection"]["values"] = postal_codes
    response = requests.post(url=url, json=query)
    content = response.json()
    result = {}
    for area in content["data"]:
        code = area["key"][0]
        result[code] = area["values"]
    columns = [column["text"] for column in content["columns"][2:]]
    df = pd.DataFrame.from_dict(result, orient="index", columns=columns)
    if df.isnull().sum() > 0:
        logging.error("Null values in the postal code info data")
    return df

def extract_apartment_price_info_for_areas(url):
    with open(os.path.join(os.path.dirname(__file__), "queries", "apartment_price_query.json"), "r") as file:
        query = json.load(file)
    response = requests.post(url, json=query)
    content = response.json()
    result = {}
    for area in content["data"]:
        result[area["key"][1]] = area["values"][0].replace(".", "")
    df = pd.DataFrame.from_dict(result, orient="index", columns=["Neliöhinta EUR/m2"])
    return df

def extract_apartment_price_info_for_municipalities(url):
    with open(os.path.join(os.path.dirname(__file__), "queries", "apartment_price_municipality_query.json"), "r") as file:
        query = json.load(file)
    response = requests.post(url, json=query)
    content = response.json()
    result = {}
    for area in content["data"]:
        result[area["key"][1]] = area["values"][0]
    muni_df = pd.DataFrame.from_dict(result, orient="index", columns=["Neliöhinta EUR/m2"])
    muni_df["municipality"] = pd.Series(data=["Espoo", "Helsinki", "Oulu", "Tampere", "Turku", "Vantaa"], 
                                   index=["049", "091", "564", "837", "853", "092"])
    return muni_df

def main():
    config = load_config()
    postal_code_mapping = extract_postal_code_mapping(config["sources"]["postal_code_url"])
    postal_code_info = extract_postal_code_info(config["sources"]["postal_code_url"], postal_code_mapping["code"].to_list())
    apartment_prices_areas = extract_apartment_price_info_for_areas(config["sources"]["apartment_prices_area_url"])
    apartment_prices_municipalities = extract_apartment_price_info_for_municipalities(config["sources"]["apartment_prices_municipality_url"])


if __name__ == "__main__":
    main()
