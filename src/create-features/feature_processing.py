import numpy as np
import pandas as pd


def calculate_population_growth(postal_code_info_latest: pd.DataFrame, postal_code_info_old: pd.DataFrame) -> pd.DataFrame:
    df = postal_code_info_latest.loc[:, ["Postal code", "Asukkaat yhteensä (HE)"]].merge(
        right=postal_code_info_old.loc[:, ["Postal code", "Asukkaat yhteensä (HE)"]], how="left", on="Postal code")
    df["Change in population"] = (df["Asukkaat yhteensä (HE)_x"] - df["Asukkaat yhteensä (HE)_y"]) / df["Asukkaat yhteensä (HE)_y"]
    df["Change in population"] = df["Change in population"].fillna(0)
    postal_code_info = postal_code_info_latest.merge(df.loc[:, ["Postal code", "Change in population"]], how="left", on="Postal code")
    return postal_code_info

def process_middle_age(postal_code_info: pd.DataFrame) -> pd.DataFrame:
    postal_code_info["Asukkaiden keski-ikä (HE)"] = postal_code_info["Asukkaiden keski-ikä (HE)"].astype(int)
    muni_middle_age = postal_code_info.groupby("municipality").apply(lambda data: (data["Asukkaiden keski-ikä (HE)"] * data["Asukkaat yhteensä (HE)"]).sum() / data["Asukkaat yhteensä (HE)"].sum())
    muni_middle_age_df = pd.DataFrame(muni_middle_age, columns=["Muni middle age"]).reset_index("municipality")
    postal_code_info = postal_code_info.merge(muni_middle_age_df, on="municipality", how="left")
    postal_code_info["Middle age"] = np.where(postal_code_info["Asukkaiden keski-ikä (HE)"] == 0, postal_code_info["Muni middle age"], postal_code_info["Asukkaiden keski-ikä (HE)"])
    return postal_code_info

def process_student_ratio(postal_code_info: pd.DataFrame) -> pd.DataFrame:
    postal_code_info["Student ratio"] = postal_code_info["Opiskelijat (PT)"].astype(int) / postal_code_info["Asukkaat yhteensä (HE)"].astype(int)
    postal_code_info["Student ratio"] = postal_code_info["Student ratio"].fillna(0)
    return postal_code_info

def process_unemployment_rate(postal_code_info: pd.DataFrame) -> pd.DataFrame:
    postal_code_info["Työttömät (PT)"] = postal_code_info["Työttömät (PT)"].astype(int)
    postal_code_info["Työlliset (PT)"] = postal_code_info["Työlliset (PT)"].astype(int)
    muni_unemployed_rate = postal_code_info.groupby("municipality").apply(lambda data: data["Työttömät (PT)"].sum() / (data["Työlliset (PT)"].sum() + data["Työttömät (PT)"].sum()))
    muni_unemployed_rate_df = pd.DataFrame(muni_unemployed_rate, columns=["Muni unemployment rate"]).reset_index("municipality")
    postal_code_info = postal_code_info.merge(muni_unemployed_rate_df, on="municipality", how="left")
    postal_code_info["Unemployment rate"] = np.where(postal_code_info["Asukkaat yhteensä (HE)"] != 0, postal_code_info["Työttömät (PT)"] / (postal_code_info["Työttömät (PT)"] + postal_code_info["Työlliset (PT)"]), postal_code_info["Muni unemployment rate"])
    postal_code_info["Unemployment rate"] = postal_code_info["Unemployment rate"].fillna(0)
    return postal_code_info

def process_pensioner_ratio(postal_code_info: pd.DataFrame) -> pd.DataFrame:
    postal_code_info["Pensioner ratio"] = postal_code_info["Eläkeläiset (PT)"].astype(int) / postal_code_info["Asukkaat yhteensä (HE)"]
    postal_code_info["Pensioner ratio"] = postal_code_info["Pensioner ratio"].fillna(0)
    return postal_code_info

def process_housing_density(postal_code_info: pd.DataFrame) -> pd.DataFrame:
    postal_code_info["Asumisväljyys (TE)"] = postal_code_info["Asumisväljyys (TE)"].astype(float)
    muni_housing_density = postal_code_info.groupby("municipality").apply(lambda data: (data["Asukkaat yhteensä (HE)"] * data["Asumisväljyys (TE)"]).sum() / data["Asukkaat yhteensä (HE)"].sum())
    muni_housing_density_df = pd.DataFrame(muni_housing_density, columns=["Muni housing density"]).reset_index("municipality")
    postal_code_info = postal_code_info.merge(muni_housing_density_df, how="left", on="municipality")
    postal_code_info["Housing density"] = np.where(postal_code_info["Asumisväljyys (TE)"] == 0, postal_code_info["Muni housing density"], postal_code_info["Asumisväljyys (TE)"])
    return postal_code_info

def process_population_density(postal_code_info: pd.DataFrame) -> pd.DataFrame:
    postal_code_info["Population density"] = postal_code_info["Asukkaat yhteensä (HE)"] / postal_code_info["Postinumeroalueen pinta-ala"] 
    return postal_code_info

def process_median_income(postal_code_info: pd.DataFrame) -> pd.DataFrame:
    postal_code_info["Asukkaiden mediaanitulot (HR)"] = postal_code_info["Asukkaiden mediaanitulot (HR)"].astype(float)
    muni_median_income = postal_code_info.groupby("municipality").apply(lambda data: (data["Asukkaat yhteensä (HE)"] * data["Asukkaiden mediaanitulot (HR)"]).sum() / data["Asukkaat yhteensä (HE)"].sum())
    muni_median_income_df = pd.DataFrame(muni_median_income, columns=["Muni median income"]).reset_index("municipality")
    postal_code_info = postal_code_info.merge(muni_median_income_df, how="left", on="municipality")
    postal_code_info["Median income"] = np.where(postal_code_info["Asukkaiden mediaanitulot (HR)"] == 0, postal_code_info["Muni median income"], postal_code_info["Asukkaiden mediaanitulot (HR)"])
    return postal_code_info

def process_higher_education(postal_code_info: pd.DataFrame) -> pd.DataFrame:
    postal_code_info["Higher education ratio"] = (postal_code_info["Ylemmän korkeakoulututkinnon suorittaneet (KO)"].astype(int) + postal_code_info["Alemman korkeakoulututkinnon suorittaneet (KO)"].astype(int)) / postal_code_info["Asukkaat yhteensä (HE)"]
    postal_code_info["Higher education ratio"] = postal_code_info["Higher education ratio"].fillna(0)
    return postal_code_info

def process_rented_apartments(postal_code_info: pd.DataFrame) -> pd.DataFrame:
    postal_code_info["Households living on rent ratio"] = postal_code_info["Vuokra-asunnoissa asuvat taloudet (TE)"].astype(int) / postal_code_info["Taloudet yhteensä (TE)"].astype(int)
    postal_code_info["Households living on rent ratio"] = postal_code_info["Households living on rent ratio"].fillna(0)
    return postal_code_info

def process_block_apartments(postal_code_info: pd.DataFrame) -> pd.DataFrame:
    postal_code_info["Block apartment ratio"] = postal_code_info["Kerrostaloasunnot (RA)"].astype(int) / postal_code_info["Asunnot (RA)"].astype(int)
    postal_code_info["Block apartment ratio"] = postal_code_info["Block apartment ratio"].fillna(0)
    return postal_code_info

def process_apartment_prices(apartment_prices_areas: pd.DataFrame, apartment_prices_municipalities: pd.DataFrame, postal_code_mapping: pd.DataFrame) -> pd.DataFrame:
    apartment_prices = postal_code_mapping.merge(apartment_prices_areas, how="left", on="Postal code").fillna(0)
    apartment_prices = apartment_prices.merge(apartment_prices_municipalities, how="left", on="municipality")
    apartment_prices["Apartment price"] = np.where(apartment_prices["Neliöhinta EUR/m2_x"] == 0, apartment_prices["Neliöhinta EUR/m2_y"], apartment_prices["Neliöhinta EUR/m2_x"])
    apartment_prices = apartment_prices.loc[:,["Postal code", "Apartment price"]]
    return apartment_prices
