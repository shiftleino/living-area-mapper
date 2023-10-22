import numpy as np
import pandas as pd


def calculate_population_growth(postal_code_info_latest: pd.DataFrame, postal_code_info_old: pd.DataFrame) -> pd.DataFrame:
    """Creates the feature for population growth in the postal code area in last 5 years. The growth is calculated by taking
    the difference between the current population and the population 5 years ago, and dividing the difference with the
    area's population 5 years ago. Null values (e.g., due to dividing by zero) are treated as zeros, essentially meaning 
    no change in population.
    """
    df = postal_code_info_latest.loc[:, ["Postal code", "Asukkaat yhteensä (HE)"]].merge(
        right=postal_code_info_old.loc[:, ["Postal code", "Asukkaat yhteensä (HE)"]], how="left", on="Postal code")
    df["Change in population"] = (df["Asukkaat yhteensä (HE)_x"] - df["Asukkaat yhteensä (HE)_y"]) / df["Asukkaat yhteensä (HE)_y"]
    df["Change in population"] = df["Change in population"].fillna(0)
    postal_code_info = postal_code_info_latest.merge(df.loc[:, ["Postal code", "Change in population"]], how="left", on="Postal code")
    return postal_code_info

def process_middle_age(postal_code_info: pd.DataFrame) -> pd.DataFrame:
    """Creates the feature for the middle age of the postal code area. If the area doesn't have value for the middle age,
    the middle age of the municipality will be used for the area instead.
    """
    postal_code_info["Asukkaiden keski-ikä (HE)"] = postal_code_info["Asukkaiden keski-ikä (HE)"].astype(int)
    muni_middle_age = postal_code_info.groupby("municipality").apply(lambda data: (data["Asukkaiden keski-ikä (HE)"] * data["Asukkaat yhteensä (HE)"]).sum() / data["Asukkaat yhteensä (HE)"].sum())
    muni_middle_age_df = pd.DataFrame(muni_middle_age, columns=["Muni middle age"]).reset_index("municipality")
    postal_code_info = postal_code_info.merge(muni_middle_age_df, on="municipality", how="left")
    postal_code_info["Middle age"] = np.where(postal_code_info["Asukkaiden keski-ikä (HE)"] == 0, postal_code_info["Muni middle age"], postal_code_info["Asukkaiden keski-ikä (HE)"])
    return postal_code_info

def process_student_ratio(postal_code_info: pd.DataFrame) -> pd.DataFrame:
    """Creates the feature for student ratio in the postal code area. This is calculated by dividing the number of students
    in the area by the population of the area. Missing values are filled with zeros.
    """
    postal_code_info["Student ratio"] = postal_code_info["Opiskelijat (PT)"].astype(int) / postal_code_info["Asukkaat yhteensä (HE)"].astype(int)
    postal_code_info["Student ratio"] = postal_code_info["Student ratio"].fillna(0)
    return postal_code_info

def process_unemployment_rate(postal_code_info: pd.DataFrame) -> pd.DataFrame:
    """Creates the feature for unemployment rate of the postal code area. This is calculated by dividing the number of unemployed
    people in the area by the sum of unemployed and employed people in the area. Therefore, only people who are part of the workforce
    are taken into account in this feature. If the area doesn't have specified values for unemployed and employed people, the
    unemployment rate of the municipality will be used for the area.
    """
    postal_code_info["Työttömät (PT)"] = postal_code_info["Työttömät (PT)"].astype(int)
    postal_code_info["Työlliset (PT)"] = postal_code_info["Työlliset (PT)"].astype(int)
    muni_unemployed_rate = postal_code_info.groupby("municipality").apply(lambda data: data["Työttömät (PT)"].sum() / (data["Työlliset (PT)"].sum() + data["Työttömät (PT)"].sum()))
    muni_unemployed_rate_df = pd.DataFrame(muni_unemployed_rate, columns=["Muni unemployment rate"]).reset_index("municipality")
    postal_code_info = postal_code_info.merge(muni_unemployed_rate_df, on="municipality", how="left")
    postal_code_info["Unemployment rate"] = np.where(postal_code_info["Asukkaat yhteensä (HE)"] != 0, postal_code_info["Työttömät (PT)"] / (postal_code_info["Työttömät (PT)"] + postal_code_info["Työlliset (PT)"]), postal_code_info["Muni unemployment rate"])
    postal_code_info["Unemployment rate"] = postal_code_info["Unemployment rate"].fillna(0)
    return postal_code_info

def process_pensioner_ratio(postal_code_info: pd.DataFrame) -> pd.DataFrame:
    """Creates the feature for pensioner ratio in the postal code area. This is calculated by dividing the number of pensioners
    in the area by the population of the area. Missing values are filled with zeros.
    """
    postal_code_info["Pensioner ratio"] = postal_code_info["Eläkeläiset (PT)"].astype(int) / postal_code_info["Asukkaat yhteensä (HE)"]
    postal_code_info["Pensioner ratio"] = postal_code_info["Pensioner ratio"].fillna(0)
    return postal_code_info

def process_housing_density(postal_code_info: pd.DataFrame) -> pd.DataFrame:
    """Creates the feature for housing density. If the postal code area doesn't have any value for this in the data, the
    average value of the whole municipality will be used as the feature value for the area.
    """
    postal_code_info["Asumisväljyys (TE)"] = postal_code_info["Asumisväljyys (TE)"].astype(float)
    muni_housing_density = postal_code_info.groupby("municipality").apply(lambda data: (data["Asukkaat yhteensä (HE)"] * data["Asumisväljyys (TE)"]).sum() / data["Asukkaat yhteensä (HE)"].sum())
    muni_housing_density_df = pd.DataFrame(muni_housing_density, columns=["Muni housing density"]).reset_index("municipality")
    postal_code_info = postal_code_info.merge(muni_housing_density_df, how="left", on="municipality")
    postal_code_info["Housing density"] = np.where(postal_code_info["Asumisväljyys (TE)"] == 0, postal_code_info["Muni housing density"], postal_code_info["Asumisväljyys (TE)"])
    return postal_code_info

def process_population_density(postal_code_info: pd.DataFrame) -> pd.DataFrame:
    """Creates the feature for population density by dividing the population amount by the surface area of the postal code area.
    """
    postal_code_info["Population density"] = postal_code_info["Asukkaat yhteensä (HE)"] / postal_code_info["Postinumeroalueen pinta-ala"] 
    return postal_code_info

def process_median_income(postal_code_info: pd.DataFrame) -> pd.DataFrame:
    """Creates the feature for median income of the postal code area. If the postal code area doesn't have any value for this 
    in the data, the average value of the whole municipality will be used as the feature value for the area.
    """
    postal_code_info["Asukkaiden mediaanitulot (HR)"] = postal_code_info["Asukkaiden mediaanitulot (HR)"].astype(float)
    muni_median_income = postal_code_info.groupby("municipality").apply(lambda data: (data["Asukkaat yhteensä (HE)"] * data["Asukkaiden mediaanitulot (HR)"]).sum() / data["Asukkaat yhteensä (HE)"].sum())
    muni_median_income_df = pd.DataFrame(muni_median_income, columns=["Muni median income"]).reset_index("municipality")
    postal_code_info = postal_code_info.merge(muni_median_income_df, how="left", on="municipality")
    postal_code_info["Median income"] = np.where(postal_code_info["Asukkaiden mediaanitulot (HR)"] == 0, postal_code_info["Muni median income"], postal_code_info["Asukkaiden mediaanitulot (HR)"])
    return postal_code_info

def process_higher_education(postal_code_info: pd.DataFrame) -> pd.DataFrame:
    """Creates the feature for higher education ratio of the population in the postal code area. Higher education is defined as
    either a university degree (Bachelor's or Master's) or a polytechnic degree. The ratio is calculated by dividing the amount of
    people with a higher education degree by the population of the postal code area. Missing values are replaced with zeros.
    """
    postal_code_info["Higher education ratio"] = (postal_code_info["Ylemmän korkeakoulututkinnon suorittaneet (KO)"].astype(int) + postal_code_info["Alemman korkeakoulututkinnon suorittaneet (KO)"].astype(int)) / postal_code_info["Asukkaat yhteensä (HE)"]
    postal_code_info["Higher education ratio"] = postal_code_info["Higher education ratio"].fillna(0)
    return postal_code_info

def process_rented_apartments(postal_code_info: pd.DataFrame) -> pd.DataFrame:
    """Creates the feature for the ratio of households living in rented apartments of all the households in the postal code area.
    Missing values are replaced with zeros.
    """
    postal_code_info["Households living on rent ratio"] = postal_code_info["Vuokra-asunnoissa asuvat taloudet (TE)"].astype(int) / postal_code_info["Taloudet yhteensä (TE)"].astype(int)
    postal_code_info["Households living on rent ratio"] = postal_code_info["Households living on rent ratio"].fillna(0)
    return postal_code_info

def process_block_apartments(postal_code_info: pd.DataFrame) -> pd.DataFrame:
    """Creates the feature for the ratio of block apartments of all of the apartments in the postal code area. Missing values are replaced
    with zeros.
    """
    postal_code_info["Block apartment ratio"] = postal_code_info["Kerrostaloasunnot (RA)"].astype(int) / postal_code_info["Asunnot (RA)"].astype(int)
    postal_code_info["Block apartment ratio"] = postal_code_info["Block apartment ratio"].fillna(0)
    return postal_code_info

def process_apartment_prices(apartment_prices_areas: pd.DataFrame, apartment_prices_municipalities: pd.DataFrame, postal_code_mapping: pd.DataFrame) -> pd.DataFrame:
    """Creates the feature for apartment prices of the postal code area. If the postal code area doesn't have a value for the apartment price,
    the apartment price value of the whole municipality will be used as the feature value.
    """
    apartment_prices = postal_code_mapping.merge(apartment_prices_areas, how="left", on="Postal code").fillna(0)
    apartment_prices = apartment_prices.merge(apartment_prices_municipalities, how="left", on="municipality")
    apartment_prices["Apartment price"] = np.where(apartment_prices["Neliöhinta EUR/m2_x"] == 0, apartment_prices["Neliöhinta EUR/m2_y"], apartment_prices["Neliöhinta EUR/m2_x"])
    apartment_prices = apartment_prices.loc[:,["Postal code", "Apartment price"]]
    return apartment_prices
