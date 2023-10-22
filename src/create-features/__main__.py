import pandas as pd
import numpy as np
from sklearn import preprocessing
import feature_processing


def preprocess_data(df: pd.DataFrame) -> pd.DataFrame:
    df["Postal code"] = df["Postal code"].astype(str).str.pad(width=5, side='left', fillchar="0")
    df = df.replace(["...", "..", "."], 0).fillna(0)
    return df

def normalize_features(features: pd.DataFrame) -> pd.DataFrame:
    features["Change in population"] = np.where(features["Change in population"] > 2, 2, features["Change in population"])
    with np.errstate(divide="ignore"):
        features["Population density"] = np.where(features["Population density"] > 0, np.log(features["Population density"]), features["Population density"])
    feature_array = features.iloc[:,2:].to_numpy()
    scaler = preprocessing.StandardScaler()
    standard_features = scaler.fit_transform(feature_array)
    features.iloc[:, 2:] = standard_features
    return features

def main():
    postal_code_mapping = pd.read_csv("~/dev/uni/tkt/living-area-mapper/data/postal_code_mapping.csv", sep=";")
    postal_code_mapping = preprocess_data(postal_code_mapping)
    postal_code_info_latest = pd.read_csv("~/dev/uni/tkt/living-area-mapper/data/postal_code_info_latest.csv", sep=";")
    postal_code_info_latest = preprocess_data(postal_code_info_latest)
    postal_code_info_old = pd.read_csv("~/dev/uni/tkt/living-area-mapper/data/postal_code_info_old.csv", sep=";")
    postal_code_info_old = preprocess_data(postal_code_info_old)
    apartment_prices_areas = pd.read_csv("~/dev/uni/tkt/living-area-mapper/data/apartment_prices_areas.csv", sep=";")
    apartment_prices_municipalities = pd.read_csv("~/dev/uni/tkt/living-area-mapper/data/apartment_prices_municipalities.csv", sep=";")
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
    
    raw_data = postal_code_info.loc[:, ["Postal code", "municipality", "Apartment price", "Change in population",
                                    "Middle age", "Student ratio", "Unemployment rate", "Pensioner ratio", "Housing density", 
                                    "Population density", "Median income", "Higher education ratio", "Households living on rent ratio", 
                                    "Block apartment ratio"]]
    features = postal_code_info.loc[:, ["Postal code", "municipality", "Apartment price", "Change in population", "Middle age",
                                    "Student ratio", "Unemployment rate", "Pensioner ratio", "Housing density", "Population density", 
                                    "Median income", "Higher education ratio", "Households living on rent ratio", "Block apartment ratio"]]

    for municipality in postal_code_info["municipality"].unique():
        muni_features = normalize_features(features.loc[features["municipality"] == municipality,:].copy())
        muni_features.to_csv(f"~/dev/uni/tkt/living-area-mapper/data/{municipality}_features.csv", sep=";", index=False)
    raw_data.to_csv("~/dev/uni/tkt/living-area-mapper/data/raw_data.csv", sep=";", index=False)


if __name__ == "__main__":
    main()
