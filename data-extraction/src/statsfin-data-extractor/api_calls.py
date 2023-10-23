import pandas as pd
import os
import json
import requests
import logging
import re
import csv


def extract_postal_code_mapping(url: str) -> pd.DataFrame:
    """Extract the postal codes and their names from StatsFin API.
    """
    with open(os.path.join(os.path.dirname(__file__), "queries", "postal_codes_query.json"), "r") as file:
        query = json.load(file)
    response = requests.post(url=url, json=query)
    if response.status_code != 200:
        logging.error(f"Request to Postal code API server failed with status code: {response.status_code}")
        raise Exception("No response from API")
    content = response.text
    postal_codes_combined = [re.split("\s+", line[0], 1) for line in list(csv.reader(content.splitlines(), delimiter=','))[2:]]
    postal_code_mapping = [[pair[0], pair[1].split("(")[0].strip(),  pair[1].split("(")[1].replace(")", "")] for pair in postal_codes_combined]
    df = pd.DataFrame(postal_code_mapping, columns=["Postal code", "name", "municipality"])
    filtered_df = df.loc[df["municipality"].isin(["Helsinki", "Espoo", "Tampere", "Oulu", "Vantaa", "Turku"]),:]
    return filtered_df

def extract_postal_code_info(url: str, postal_codes: list[str], year: str) -> pd.DataFrame:
    """Extract information related to postal codes of the specified year from StatsFin API.
    """
    with open(os.path.join(os.path.dirname(__file__), "queries", "postal_area_basics_query.json"), "r") as file:
        query = json.load(file)
    query["query"][0]["selection"]["values"] = postal_codes
    query["query"][2]["selection"]["values"] = [year]
    response = requests.post(url=url, json=query)
    if response.status_code != 200:
        logging.error(f"Request to Postal code API server failed with status code: {response.status_code}")
        raise Exception("No response from API")
    content = response.json()
    result = {}
    for area in content["data"]:
        code = area["key"][0]
        result[code] = area["values"]
    columns = [column["text"] for column in content["columns"][2:]]
    df = pd.DataFrame.from_dict(result, orient="index", columns=columns)
    return df

def extract_apartment_price_info_for_areas(url: str) -> pd.DataFrame:
    """Extract apartment price information of postal code areas from StatsFin API.
    """
    with open(os.path.join(os.path.dirname(__file__), "queries", "apartment_price_query.json"), "r") as file:
        query = json.load(file)
    response = requests.post(url, json=query)
    if response.status_code != 200:
        logging.error(f"Request to Postal code API server failed with status code: {response.status_code}")
        raise Exception("No response from API")
    content = response.json()
    result = {}
    for area in content["data"]:
        result[area["key"][1]] = area["values"][0].replace(".", "")
    df = pd.DataFrame.from_dict(result, orient="index", columns=["Neliöhinta EUR/m2"])
    return df

def extract_apartment_price_info_for_municipalities(url: str) -> pd.DataFrame:
    """Extract apartment price information of municipalities from StatsFin API.
    """
    with open(os.path.join(os.path.dirname(__file__), "queries", "apartment_price_municipality_query.json"), "r") as file:
        query = json.load(file)
    response = requests.post(url, json=query)
    if response.status_code != 200:
        logging.error(f"Request to Postal code API server failed with status code: {response.status_code}")
        raise Exception("No response from API")
    content = response.json()
    result = {}
    for area in content["data"]:
        result[area["key"][1]] = area["values"][0]
    muni_df = pd.DataFrame.from_dict(result, orient="index", columns=["Neliöhinta EUR/m2"])
    muni_df["municipality"] = pd.Series(data=["Espoo", "Helsinki", "Oulu", "Tampere", "Turku", "Vantaa"], 
                                   index=["049", "091", "564", "837", "853", "092"])
    return muni_df
