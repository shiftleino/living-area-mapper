import logging
import tomllib
import os
from datetime import datetime
from api_calls import extract_apartment_price_info_for_areas, \
        extract_apartment_price_info_for_municipalities, extract_postal_code_info, \
        extract_postal_code_mapping


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

def load_config():
    """Load configuration details related to URLs.
    """
    with open(os.path.join(os.path.dirname(__file__), "config.toml"), mode="rb") as file:
        config = tomllib.load(file)
    return config

def main():
    config = load_config()
    current_year = datetime.now().year
    postal_code_mapping = extract_postal_code_mapping(config["sources"]["postal_code_url"])
    postal_code_info_latest = extract_postal_code_info(config["sources"]["postal_code_url"], postal_code_mapping["Postal code"].to_list(), str(current_year - 2))
    postal_code_info_old = extract_postal_code_info(config["sources"]["postal_code_url"], postal_code_mapping["Postal code"].to_list(), str(current_year - 7))
    apartment_prices_areas = extract_apartment_price_info_for_areas(config["sources"]["apartment_prices_area_url"], str(current_year - 1))
    apartment_prices_municipalities = extract_apartment_price_info_for_municipalities(config["sources"]["apartment_prices_municipality_url"], str(current_year - 1))

    postal_code_info_latest.to_csv("../data/postal_code_info_latest.csv", index=True, sep=";", index_label="Postal code")
    postal_code_info_old.to_csv("../data/postal_code_info_old.csv", index=True, sep=";", index_label="Postal code")
    apartment_prices_areas.to_csv("../data/apartment_prices_areas.csv", index=True, sep=";", index_label="Postal code")
    apartment_prices_municipalities.to_csv("../data/apartment_prices_municipalities.csv", index=True, sep=";")
    postal_code_mapping.to_csv("../data/postal_code_mapping.csv", index=False, sep=";")


if __name__ == "__main__":
    main()
