import argparse
import datetime
import math
import matplotlib.pyplot as plt
import os
import pandas as pd
import sys
import seaborn as sn

from numpy.core.numeric import NaN
from typing import Iterable, Union

# Data set filepath and column information for the cattle data set
CATTLE_DATA_FILEPATH = "../data/individual_data_sets/cattle_data/cattle-livestock-count-heads.csv"
COUNTRY_CATTLE_COL = "Entity"
COUNTRY_CODE_CATTLE_COL = "Code"
CATTLE_YEAR_COL = "Year"
NUM_OF_CATTLE_COL = "Live Animals - Cattle - 866 - Stocks - 5111 - Head" 

# Data set filepath and column information for the promed data set
CCHF_PROMED_DATA_FILEPATH = "../data/individual_data_sets/CCHF_data/cchf_data.csv"
DISEASE_NAME_COL = "diseasename"
PLACE_OF_DISEASE_COL = "place"
COUNTRY_PROMED_COL = "country"
COUNTRY_PROMED_LAT_COL = "lat"
COUNTRY_PROMED_LON_COL = "lon"
CCHF_SUMMRY_COL = "summary"
PROMED_ISSUE_DATE_COL = "issue_date"
CCHF_CITY_OR_REGION_COL = "region/city"
CCHF_CITY_OR_REGION_LAT_COL = "region/city lat"
CCHF_CITY_OR_REGION_LON_COL = "region/city lon"
CCHF_NUM_OF_CASES_COL = "cases"
CCHF_NUM_OF_DEATHS_COL = "deaths"
CCHF_TOTAL_NUM_OF_CASES_COL = "total cases"
CCHF_TOTAL_NUM_OF_DEATHS_COL = "total deaths"
CCHF_YEAR_COL = "year"

# Combined Promed and Cattle data
NUM_OF_CATTLE_WITH_PROMED_COL = "Num of cattle"

# Data Set information for the population data
POPULATION_DATA_FILEPATH = "../data/individual_data_sets/population_data/population_data_countries.csv"
POPULATION_COUNTRY_NAME_COL = "Country Name"

# Combined population and Promed Data
POPULATION_DATA_COL = "Population"

# Directory Paths
PLOTS_DIR = "../plots"

# Inclusion of cattle data
INCLUDE_CATTLE_DATA = True

def main():

  cattle_df = retrieve_data(filepath=CATTLE_DATA_FILEPATH)
  cchf_df = retrieve_data(filepath=CCHF_PROMED_DATA_FILEPATH)
  population_df = retrieve_data(filepath=POPULATION_DATA_FILEPATH)
  yearly_cchf_data = cchf_yearly_cases_and_deaths_df(cchf_df=cchf_df)

  # assign yearly cchf data to avoid dual pathing
  combined_df = yearly_cchf_data

  if INCLUDE_CATTLE_DATA:
    combined_df = combine_promed_and_cattle_data(
      cchf_df = yearly_cchf_data,
      cattle_df = cattle_df
    )

  combined_df = combine_promed_and_population_data(
    cchf_df = combined_df,
    population_df = population_df
  )
  
  interested_cols = [
    CCHF_YEAR_COL, 
    CCHF_TOTAL_NUM_OF_CASES_COL, 
    CCHF_TOTAL_NUM_OF_DEATHS_COL, 
    POPULATION_DATA_COL
  ]

  if INCLUDE_CATTLE_DATA:
    interested_cols.append(NUM_OF_CATTLE_WITH_PROMED_COL)

  combined_df.to_csv(f"complete_data.csv", index=False)

  analyze_combined_data(
    combined_data = combined_df,
    columns_to_comp = interested_cols,
    include_cattle_data = INCLUDE_CATTLE_DATA
  )

def retrieve_data(filepath: str) -> pd.DataFrame:
  return pd.read_csv(filepath)

def cchf_yearly_cases_and_deaths_df(cchf_df: pd.DataFrame) -> pd.DataFrame:
  """
  Name: format_cchf_df

  Purpose: Formats the CCHF data so the data is on a yearly basis per country
           instead of an per notification issued basis
  
  Input:

  Output:
  """

  cchf_yearly_info = {}

  for idx, row in cchf_df.iterrows():

    disease = row[DISEASE_NAME_COL]
    country = row[COUNTRY_PROMED_COL]
    promed_notice_issue_date = row[PROMED_ISSUE_DATE_COL]
    cases_for_notice = row[CCHF_NUM_OF_CASES_COL]
    deaths_for_notice = row[CCHF_NUM_OF_DEATHS_COL]
    total_cases_that_year = row[CCHF_TOTAL_NUM_OF_CASES_COL]
    total_deaths_that_year = row[CCHF_TOTAL_NUM_OF_DEATHS_COL]

    # First we need to figure out the year we are currently looking at for this notification
    datem = datetime.datetime.strptime(promed_notice_issue_date, "%Y-%m-%d")
    year_of_interest = str(datem.year)

    """
    Now that we have our year we are going to construct a dictionary to help
    store our information collectively. This will be helpful when we go to reduce
    the actual map into a csv format
    """

    # First we shall check if information exists already and if not create it
    if country not in cchf_yearly_info:
      cchf_yearly_info[country] = {}
    
    if disease not in cchf_yearly_info[country]:
      cchf_yearly_info[country][disease] = {}

    if year_of_interest not in cchf_yearly_info[country][disease]:

      num_of_cases = cases_for_notice
      if not math.isnan(total_cases_that_year):
        num_of_cases = total_cases_that_year

      num_of_deaths = deaths_for_notice
      if not math.isnan(total_deaths_that_year):
        num_of_deaths = total_deaths_that_year

      if math.isnan(num_of_deaths):
        num_of_deaths = 0
      
      if math.isnan(num_of_cases):
        num_of_cases = 0

      cchf_yearly_info[country][disease][year_of_interest] = {
        CCHF_TOTAL_NUM_OF_CASES_COL: num_of_cases,
        CCHF_TOTAL_NUM_OF_DEATHS_COL: num_of_deaths,
        PROMED_ISSUE_DATE_COL: promed_notice_issue_date,
      }
      continue

    """
    If our data does exist in our map then we need to do the following:

      1. Compare the date the notice was issued to the most recent record of 
         of the promed notification. This is in case a situation arises where
         a data source is out of order and the total for a particular year is seen
         first

      2a. If the date the notice was issued is before our previously recorded record
          we shall discard it since we are assuming it has been accounted for
      
      2b. If the date is after the previously recorded record then we shall continue
          to the next steps

      3.  We then check the total cases column and the total deaths column as they will
          supercede our previous records results for that year and subsequently overwrite
          them and update the record's issue date

      4.  If one of the total columns is not valid then we shall simply add the number of cases
          to our total from that year and replace the issue date
    """
    country_diease_year_info = cchf_yearly_info[country][disease][year_of_interest]

    if promed_notice_issue_date > country_diease_year_info[PROMED_ISSUE_DATE_COL]:

      if math.isnan(deaths_for_notice):
        deaths_for_notice = 0
      
      if math.isnan(cases_for_notice):
        cases_for_notice = 0

      if not math.isnan(total_cases_that_year):
        country_diease_year_info[CCHF_TOTAL_NUM_OF_CASES_COL] = total_cases_that_year
      else:
        country_diease_year_info[CCHF_TOTAL_NUM_OF_CASES_COL] += cases_for_notice
      
      if not math.isnan(total_deaths_that_year):
        country_diease_year_info[CCHF_TOTAL_NUM_OF_DEATHS_COL] = total_deaths_that_year
      else:
        country_diease_year_info[CCHF_TOTAL_NUM_OF_DEATHS_COL] += deaths_for_notice

      country_diease_year_info[PROMED_ISSUE_DATE_COL] = promed_notice_issue_date

  """
  Now that we have successfully mapped our data data to the appropriate year
  we simply need to reduce our data mapping
  """
  cchf_yearly_cases_and_deaths_data = {
    COUNTRY_PROMED_COL: [],
    DISEASE_NAME_COL: [],
    CCHF_YEAR_COL: [],
    CCHF_TOTAL_NUM_OF_CASES_COL: [],
    CCHF_TOTAL_NUM_OF_DEATHS_COL: []
  }

  for country_key, country_val in cchf_yearly_info.items():

    for disease_key, disease_val in country_val.items():

      for year_key, year_val in disease_val.items():
        cchf_yearly_cases_and_deaths_data[DISEASE_NAME_COL].append(disease_key)        
        cchf_yearly_cases_and_deaths_data[COUNTRY_PROMED_COL].append(country_key)
        cchf_yearly_cases_and_deaths_data[CCHF_YEAR_COL].append(year_key)
        cchf_yearly_cases_and_deaths_data[CCHF_TOTAL_NUM_OF_CASES_COL].append(year_val[CCHF_TOTAL_NUM_OF_CASES_COL])
        cchf_yearly_cases_and_deaths_data[CCHF_TOTAL_NUM_OF_DEATHS_COL].append(year_val[CCHF_TOTAL_NUM_OF_DEATHS_COL])

  return pd.DataFrame(cchf_yearly_cases_and_deaths_data)

def combine_promed_and_cattle_data(cchf_df: pd.DataFrame, cattle_df: pd.DataFrame) -> pd.DataFrame:

  number_of_cattle = []

  for idx, row in cchf_df.iterrows():

    year = row[CCHF_YEAR_COL]
    country = row[COUNTRY_PROMED_COL]

    num_of_cattle = cattle_df.query(f"{COUNTRY_CATTLE_COL}==\"{country}\"&{CATTLE_YEAR_COL}=={year}")

    try:
      number_of_cattle.append(num_of_cattle[NUM_OF_CATTLE_COL].values[0])
    except Exception as err:
      number_of_cattle.append(NaN)
  
  cchf_df[NUM_OF_CATTLE_WITH_PROMED_COL] = number_of_cattle

  return cchf_df

def combine_promed_and_population_data(cchf_df: pd.DataFrame, population_df: pd.DataFrame) -> pd.DataFrame:

  population_data = []

  # First get all of the countries listed in our combined dataframe
  for idx, row in cchf_df.iterrows():
    country = row[COUNTRY_PROMED_COL].strip()
    year = row[CCHF_YEAR_COL]
    
    country_population_df = population_df[population_df[POPULATION_COUNTRY_NAME_COL] == country]
    
    # Since there should be only one entry per country we can simple do an iloc[0]
    population = country_population_df.iloc[0][year]
    population_data.append(population)

  cchf_df[POPULATION_DATA_COL] = population_data
  
  return cchf_df

def analyze_combined_data(combined_data: pd.DataFrame, columns_to_comp: list, include_cattle_data: bool) -> None:

  combined_data.dropna(inplace=True)

  countries_in_df = set(combined_data[COUNTRY_PROMED_COL].values)
  
  for country in countries_in_df:
    country_combined_df = combined_data[combined_data[COUNTRY_PROMED_COL] == country]

    # Grab the data we only care about which is the year, total cases, total deaths and the number of cattle
    fiiltered_df = country_combined_df[columns_to_comp]
    print(fiiltered_df.corr())
    # Generate a visual representation of the Correlation Matrix
    sn.heatmap(fiiltered_df.corr(), annot=True)
    if not os.path.isdir(PLOTS_DIR):
      os.mkdir(PLOTS_DIR)

    country_data_name = f"{country}"
    if include_cattle_data:
      country_data_name += "_includes_cattle_data"
    file_name = f"{PLOTS_DIR}/{country_data_name}_data_correlation_matrix.png"

    plt.title(f"{country} Correlation Matrix")
    plt.savefig(f"{file_name}")
    plt.clf()

if __name__ == "__main__":
    main()