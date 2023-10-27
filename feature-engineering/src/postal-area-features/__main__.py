from google.cloud import storage
import pandas as pd
import numpy as np
from sklearn import preprocessing
import feature_processing
import logging
import tomllib
import os


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

def preprocess_data(df: pd.DataFrame) -> pd.DataFrame:
    """Fix postal codes to have leading zeros and handle missing data.
    """
    df["Postal code"] = df["Postal code"].astype(str).str.pad(width=5, side='left', fillchar="0")
    df = df.replace(["...", "..", "."], 0).fillna(0)
    return df

def normalize_features(features: pd.DataFrame) -> pd.DataFrame:
    """Normalize features by scaling them to mean zero and standard deviation of one.
    In addition, clip change in population to maximum of 200% growth as there might be,
    cases in the data where there was no population before, thus causing outliers for
    values in the change in population feature. The function also applies log scaling
    for population density as this feature has a wide range of values with a long right tail,
    which should not affect too much on the end result inferences.
    """
    features["Change in population"] = np.where(features["Change in population"] > 2, 2, features["Change in population"])
    with np.errstate(divide="ignore"):
        minimum_log_pop_density = np.log(features.loc[features["Population density"] > 0, ["Population density"]]).min()
        features["Population density"] = np.where(features["Population density"] > 0, np.log(features["Population density"]), minimum_log_pop_density)
    feature_array = features.iloc[:,2:].to_numpy()
    scaler = preprocessing.StandardScaler()
    standard_features = scaler.fit_transform(feature_array)
    features.iloc[:, 2:] = standard_features
    return features

def main():
    config = load_config()
    logging.info("Creating client for authenticating to GCP")
    storage_client = storage.Client(project=config["cloud"]["project_id"])
    raw_data_bucket = storage_client.bucket(config["cloud"]["bucket_name_raw_data"])
    postal_code_mapping_blob = raw_data_bucket.blob("postal_code_mapping.csv")
    postal_code_info_latest_blob = raw_data_bucket.blob("postal_code_info_latest.csv")
    postal_code_info_old_blob = raw_data_bucket.blob("postal_code_info_old.csv")
    apartment_prices_areas_blob = raw_data_bucket.blob("apartment_prices_areas.csv")
    apartment_prices_municipalities_blob = raw_data_bucket.blob("apartment_prices_municipalities.csv")

    logging.info("Downloading the csv-files from the GCP cloud storage bucket")
    postal_code_mapping_blob.download_to_filename("postal_code_mapping.csv")
    postal_code_info_latest_blob.download_to_filename("postal_code_info_latest.csv")
    postal_code_info_old_blob.download_to_filename("postal_code_info_old.csv")
    apartment_prices_areas_blob.download_to_filename("apartment_prices_areas.csv")
    apartment_prices_municipalities_blob.download_to_filename("apartment_prices_municipalities.csv")

    logging.info("Reading the downloaded csv-files to Pandas dataframes")
    postal_code_mapping = pd.read_csv("./postal_code_mapping.csv", sep=";")
    postal_code_mapping = preprocess_data(postal_code_mapping)
    postal_code_info_latest = pd.read_csv("./postal_code_info_latest.csv", sep=";")
    postal_code_info_latest = preprocess_data(postal_code_info_latest)
    postal_code_info_old = pd.read_csv("./postal_code_info_old.csv", sep=";")
    postal_code_info_old = preprocess_data(postal_code_info_old)
    apartment_prices_areas = pd.read_csv("./apartment_prices_areas.csv", sep=";")
    apartment_prices_municipalities = pd.read_csv("./apartment_prices_municipalities.csv", sep=";")
    apartment_prices_areas = preprocess_data(apartment_prices_areas)
    
    postal_code_info = feature_processing.calculate_population_growth(postal_code_info_latest, postal_code_info_old)
    postal_code_info = postal_code_info.merge(postal_code_mapping, how="left", on="Postal code")
    postal_code_info = feature_processing.process_middle_age(postal_code_info)
    postal_code_info = feature_processing.process_student_ratio(postal_code_info)
    postal_code_info = feature_processing.process_unemployment_rate(postal_code_info)
    postal_code_info = feature_processing.process_pensioner_ratio(postal_code_info)
    postal_code_info = feature_processing.process_housing_density(postal_code_info)
    postal_code_info = feature_processing.process_population_density(postal_code_info)
    postal_code_info = feature_processing.process_median_income(postal_code_info)
    postal_code_info = feature_processing.process_higher_education(postal_code_info)
    postal_code_info = feature_processing.process_rented_apartments(postal_code_info)
    postal_code_info = feature_processing.process_block_apartments(postal_code_info)
    apartment_prices = feature_processing.process_apartment_prices(apartment_prices_areas, apartment_prices_municipalities, postal_code_mapping)
    postal_code_info = postal_code_info.merge(apartment_prices, how="left", on="Postal code")
    
    features = postal_code_info.loc[:, ["Postal code", "municipality", "Apartment price", "Change in population", "Middle age",
                                    "Student ratio", "Unemployment rate", "Pensioner ratio", "Housing density", "Population density", 
                                    "Median income", "Higher education ratio", "Households living on rent ratio", "Block apartment ratio"]]

    features_bucket = storage_client.bucket(config["cloud"]["bucket_name_features"])
    for municipality in postal_code_info["municipality"].unique():
        logging.info(f"Normalizing features for {municipality} and uploading the result as a csv-file to GCP cloud storage bucket")
        muni_features = normalize_features(features.loc[features["municipality"] == municipality,:].copy())
        muni_features_blob = features_bucket.blob(f"{municipality}_features.csv")
        muni_features_blob.upload_from_string(muni_features.to_csv(sep=";", index=False))
    
    app_data_bucket = storage_client.bucket(config["cloud"]["bucket_name_app_data"])

    logging.info("Uploading the raw data as csv-file to GCP cloud storage bucket")
    postal_code_info_blob = app_data_bucket.blob("raw_data.csv")
    postal_code_info_blob.upload_from_string(postal_code_info.to_csv(sep=",", index=False, quoting=1))


if __name__ == "__main__":
    main()
