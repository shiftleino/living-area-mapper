import pandas as pd
import numpy as np
import logging


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

def preprocess_data(df: pd.DataFrame) -> pd.DataFrame:
    """Fix postal codes to have leading zeros.
    """
    df["Postal code"] = df["Postal code"].astype(str).str.pad(width=5, side='left', fillchar="0")
    return df

def compute_mappings(all_features: list[pd.DataFrame]) -> pd.DataFrame:
    """Computes for each living area the similarity measures to other areas and selects as the closest area
    for the municipality the area with the smallest distance.
    """
    logging.info("Computing the closest living areas in each municipality for every living area.")
    result = {}
    columns = ["Helsinki", "Espoo", "Vantaa", "Turku", "Tampere", "Oulu"]

    for i in range(6):
        logging.info(f"Computing the living area mappings for areas in {columns[i]}")
        source_feature_df = all_features[i]
        source_amount = len(source_feature_df.index)
        for idx in range(source_amount):
            source_postal_code = source_feature_df.iloc[idx, 0]
            result[source_postal_code] = [None] * 6
            source_features = source_feature_df.iloc[idx,2:].to_numpy(dtype=float)
            for j in range(6):
                if j != i:
                    target_feature_df = all_features[j]
                    target_features = target_feature_df.iloc[:,2:].to_numpy(dtype=float)
                    distances = np.sqrt(((target_features - source_features)**2).sum(axis=1))
                    target_postal_code = target_feature_df.iloc[np.argmin(distances), 0]
                    result[source_postal_code][j] = target_postal_code

    inference_df = pd.DataFrame.from_dict(result, orient="index", columns=columns)
    return inference_df

def main():
    helsinki_features = pd.read_csv("~/dev/uni/tkt/living-area-mapper/data/Helsinki_features.csv", sep=";")
    helsinki_features = preprocess_data(helsinki_features)
    espoo_features = pd.read_csv("~/dev/uni/tkt/living-area-mapper/data/Espoo_features.csv", sep=";")
    espoo_features = preprocess_data(espoo_features)
    vantaa_features = pd.read_csv("~/dev/uni/tkt/living-area-mapper/data/Vantaa_features.csv", sep=";")
    vantaa_features = preprocess_data(vantaa_features)
    turku_features = pd.read_csv("~/dev/uni/tkt/living-area-mapper/data/Turku_features.csv", sep=";")
    turku_features = preprocess_data(turku_features)
    tampere_features = pd.read_csv("~/dev/uni/tkt/living-area-mapper/data/Tampere_features.csv", sep=";")
    tampere_features = preprocess_data(tampere_features)
    oulu_features = pd.read_csv("~/dev/uni/tkt/living-area-mapper/data/Oulu_features.csv", sep=";")
    oulu_features = preprocess_data(oulu_features)
    all_features = [helsinki_features, espoo_features, vantaa_features, turku_features, tampere_features, oulu_features]
    area_mappings = compute_mappings(all_features)

    area_mappings.to_csv("~/dev/uni/tkt/living-area-mapper/data/living_area_mappings.csv", sep=",", index=True, index_label="Postal code", quoting=1)


if __name__ == "__main__":
    main()
