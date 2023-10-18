import pandas as pd


def preprocess_data(df: pd.DataFrame) -> pd.DataFrame:
    df["Postal code"] = df["Postal code"].astype(str).str.pad(width=5, side='left', fillchar="0")
    df = df.replace(["...", "..", "."], 0).fillna(0)
    return df

def calculate_population_growth(postal_code_info_latest: pd.DataFrame, postal_code_info_old) -> pd.DataFrame:
    df = postal_code_info_latest.loc[:, ["Postal code", "Asukkaat yhteensä (HE)"]].merge(
        right=postal_code_info_old.loc[:, ["Postal code", "Asukkaat yhteensä (HE)"]], how="left", on="Postal code")
    df["Change in population"] = (df["Asukkaat yhteensä (HE)_x"] - df["Asukkaat yhteensä (HE)_y"]) / df["Asukkaat yhteensä (HE)_y"]
    postal_code_info = postal_code_info_latest.merge(df.loc[:, ["Postal code", "Change in population"]], how="left", on="Postal code")
    return postal_code_info

def main():
    postal_code_info_latest = pd.read_csv("~/dev/uni/tkt/living-area-mapper/data/postal_code_info_latest.csv", sep=";")
    postal_code_info_latest = preprocess_data(postal_code_info_latest)
    postal_code_info_old = pd.read_csv("~/dev/uni/tkt/living-area-mapper/data/postal_code_info_old.csv", sep=";")
    postal_code_info_old = preprocess_data(postal_code_info_old)
    postal_code_info = calculate_population_growth(postal_code_info_latest, postal_code_info_old)



if __name__ == "__main__":
    main()