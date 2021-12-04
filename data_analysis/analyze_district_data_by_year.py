import pandas as pd
import datetime
import math
import os
import sys
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sn

# Data set filepath and column information for the promed data set
CCHF_PROMED_DATA_FILEPATH = "../data/individual_data_sets/CCHF_data/cchf_district_data.csv"
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
CCHF_DISTRICT_COL = "district"

# Data set filepath and column information for the vegetation data
VGI_DATA_FILEPATH = "../data/individual_data_sets/vegetation_data/vgi_data.csv"
VGI_COUNTRY_COL = "country"
VGI_DISTRICT_COL = "district"
VGI_YEAR_COL = "year"
VGI_AVG_NVDI_VAL = "Avg. NVDI Val"

# Data set filepath and column information for Precipitation Data
PRECIPITATION_FILEPATH = "../data/individual_data_sets/precipitation_data/yearly_precipitation_data_by_district.csv"
COUNTRY_PRECIPITATION_COUNTRY_COL = "country"
COUNTRY_PRECIPITATION_DISTRICT_COL = "district" 
COUNTRY_PRECIPITATION_YEAR_COL = "year" 
COUNTRY_PRECIPITATION_COL = "PRECTOTLAND kg m-2 s-1"

# Data set filepath and column information for Temperature Data
TEMPERATURE_FILEPATH = "../data/individual_data_sets/temperature_data/yearly_temperature_data_by_district.csv"
COUNTRY_TEMPERATURE_COUNTRY_COL = "country"
COUNTRY_TEMPERATURE_DISTRICT_COL = "district" 
COUNTRY_TEMPERATURE_YEAR_COL = "year" 
COUNTRY_TEMPERATURE_COL = "temperature in (K)"

# Plots directory
PLOTS_DIR = "../plots"

def main():
  cchf_df = retrieve_data(filepath=CCHF_PROMED_DATA_FILEPATH)
  vgi_df = retrieve_data(filepath=VGI_DATA_FILEPATH)
  precipitation_df = retrieve_data(filepath=PRECIPITATION_FILEPATH)
  temperature_data = retrieve_data(filepath=TEMPERATURE_FILEPATH)

  cchf_district_df = construct_district_cchf_yearly_cases_and_deaths_df(
    cchf_df = cchf_df
  )
  cchf_district_df[CCHF_YEAR_COL] = cchf_district_df[CCHF_YEAR_COL].astype(int)

  # Merge the temperature and precipitation data and our combined data
  vgi_and_cchf_df = cchf_district_df.merge(vgi_df[[VGI_COUNTRY_COL, VGI_DISTRICT_COL, VGI_YEAR_COL, VGI_AVG_NVDI_VAL]])
  combined_df = vgi_and_cchf_df.merge(precipitation_df[[COUNTRY_PRECIPITATION_COUNTRY_COL, COUNTRY_PRECIPITATION_DISTRICT_COL, COUNTRY_PRECIPITATION_YEAR_COL, COUNTRY_PRECIPITATION_COL]])
  combined_df = combined_df.merge(temperature_data[[COUNTRY_TEMPERATURE_COUNTRY_COL, COUNTRY_TEMPERATURE_DISTRICT_COL, COUNTRY_TEMPERATURE_YEAR_COL, COUNTRY_TEMPERATURE_COL]])

  combined_df.to_csv("../data/combined_district_data.csv", index=False)

  # gen_timeseries_for_vgi_district_years(
  #   df = vgi_df,
  #   interested_cols = [VGI_YEAR_COL, VGI_DISTRICT_COL, VGI_AVG_NVDI_VAL],
  #   title="Average Vegetation Index",
  #   ylabel="Avg NVDI"
  # )

  # gen_bar_plot_for_cchf_data(
  #   df = cchf_district_df,
  #   interested_cols = [CCHF_YEAR_COL, CCHF_DISTRICT_COL, CCHF_TOTAL_NUM_OF_CASES_COL, CCHF_TOTAL_NUM_OF_DEATHS_COL],
  #   title="CCHF Cases and Deaths"
  # )

  gen_correlation_matrix_for_data(
    combined_data = combined_df,
    columns_to_comp = [CCHF_TOTAL_NUM_OF_CASES_COL, CCHF_TOTAL_NUM_OF_DEATHS_COL, VGI_AVG_NVDI_VAL, COUNTRY_TEMPERATURE_COL, COUNTRY_PRECIPITATION_COL],
    replace_nas=False
  )

def retrieve_data(filepath: str) -> pd.DataFrame:
  return pd.read_csv(filepath)

def construct_district_cchf_yearly_cases_and_deaths_df(cchf_df: pd.DataFrame) -> pd.DataFrame:
  """
  Name: format_cchf_df

  Purpose: Formats the CCHF data so the data is on a yearly basis per country
           instead of an per notification issued basis
  
  Input:

  Output:
  """

  cchf_yearly_info = {}
  district_coords_info = {}

  # Remove rows for districts we don't have data for
  cchf_df = cchf_df[cchf_df[CCHF_DISTRICT_COL].notna()]

  for idx, row in cchf_df.iterrows():

    disease = row[DISEASE_NAME_COL]
    country = row[COUNTRY_PROMED_COL]
    promed_notice_issue_date = row[PROMED_ISSUE_DATE_COL]
    cases_for_notice = row[CCHF_NUM_OF_CASES_COL]
    deaths_for_notice = row[CCHF_NUM_OF_DEATHS_COL]
    total_cases_that_year = row[CCHF_TOTAL_NUM_OF_CASES_COL]
    total_deaths_that_year = row[CCHF_TOTAL_NUM_OF_DEATHS_COL]
    district = row[CCHF_DISTRICT_COL]
    district_lat = row[CCHF_CITY_OR_REGION_LAT_COL]
    district_lon = row[CCHF_CITY_OR_REGION_LON_COL]

    # First we need to figure out the year we are currently looking at for this notification
    datem = datetime.datetime.strptime(promed_notice_issue_date, "%m/%d/%Y")
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

    if district not in cchf_yearly_info[country][disease]:
      cchf_yearly_info[country][disease][district] = {}

    if year_of_interest not in cchf_yearly_info[country][disease][district]:

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

      cchf_yearly_info[country][disease][district][year_of_interest] = {
        CCHF_TOTAL_NUM_OF_CASES_COL: num_of_cases,
        CCHF_TOTAL_NUM_OF_DEATHS_COL: num_of_deaths,
        PROMED_ISSUE_DATE_COL: promed_notice_issue_date,
      }

      # Record the district coordinates information (Pick the first city in the assigned district as coordinates)
      if district not in district_coords_info:
        district_coords_info[district] = {
          CCHF_CITY_OR_REGION_LAT_COL : district_lat,
          CCHF_CITY_OR_REGION_LON_COL : district_lon
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
    country_diease_year_info = cchf_yearly_info[country][disease][district][year_of_interest]

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
    DISEASE_NAME_COL: [],
    COUNTRY_PROMED_COL: [],
    CCHF_DISTRICT_COL: [],
    CCHF_CITY_OR_REGION_LAT_COL : [],
    CCHF_CITY_OR_REGION_LON_COL : [],
    CCHF_YEAR_COL: [],
    CCHF_TOTAL_NUM_OF_CASES_COL: [],
    CCHF_TOTAL_NUM_OF_DEATHS_COL: []
  }

  for country_key, country_val in cchf_yearly_info.items():

    for disease_key, disease_val in country_val.items():
      
      for districts_key, district_val in disease_val.items():
        
        for year_key, year_val in district_val.items():
          cchf_yearly_cases_and_deaths_data[DISEASE_NAME_COL].append(disease_key)        
          cchf_yearly_cases_and_deaths_data[COUNTRY_PROMED_COL].append(country_key)
          cchf_yearly_cases_and_deaths_data[CCHF_DISTRICT_COL].append(districts_key)
          cchf_yearly_cases_and_deaths_data[CCHF_YEAR_COL].append(year_key)
          cchf_yearly_cases_and_deaths_data[CCHF_TOTAL_NUM_OF_CASES_COL].append(year_val[CCHF_TOTAL_NUM_OF_CASES_COL])
          cchf_yearly_cases_and_deaths_data[CCHF_TOTAL_NUM_OF_DEATHS_COL].append(year_val[CCHF_TOTAL_NUM_OF_DEATHS_COL])
          cchf_yearly_cases_and_deaths_data[CCHF_CITY_OR_REGION_LAT_COL].append(district_coords_info[districts_key][CCHF_CITY_OR_REGION_LAT_COL])
          cchf_yearly_cases_and_deaths_data[CCHF_CITY_OR_REGION_LON_COL].append(district_coords_info[districts_key][CCHF_CITY_OR_REGION_LON_COL])

  return pd.DataFrame(cchf_yearly_cases_and_deaths_data)

def gen_timeseries_for_vgi_district_years(df: pd.DataFrame, interested_cols: list, title: str, ylabel: str) -> None:
  
  countries = set(df[VGI_COUNTRY_COL].to_list())

  for country in countries:
    country_df = df[df[VGI_COUNTRY_COL] == country]

    filtered_country_df = country_df[interested_cols]

    for key, grp in filtered_country_df.groupby([VGI_DISTRICT_COL]):
      fig, axs = plt.subplots()
      axs.plot(grp[VGI_YEAR_COL], grp[VGI_AVG_NVDI_VAL])

      plt.title(f"{title} for {country}'s {key} district")
      plt.ylabel(ylabel)
      plt.xlabel(VGI_YEAR_COL)
      plt.savefig(f"{PLOTS_DIR}/yearly_avg_vgi_data_for_{country}'s_{key}_district.png")
      plt.clf()

def gen_bar_plot_for_cchf_data(df: pd.DataFrame, interested_cols: list, title: str):

  WIDTH = 0.35
  countries = set(df[COUNTRY_PROMED_COL].to_list())
  
  for country in countries:
    country_df = df[df[COUNTRY_PROMED_COL] == country]

    filtered_country_df = country_df[interested_cols]

    for key, grp in filtered_country_df.groupby([CCHF_DISTRICT_COL]):
      ind = np.arange(len(grp))
      fig, axs = plt.subplots()
      axs.bar(ind, grp[CCHF_TOTAL_NUM_OF_CASES_COL], width=WIDTH, label="Num of Cases")
      axs.bar(ind + WIDTH, grp[CCHF_TOTAL_NUM_OF_DEATHS_COL], width=WIDTH, label="Num of Deaths")

      plt.title(f"{title} for {country}'s {key} district")
      plt.xlabel(CCHF_YEAR_COL)
      plt.legend(loc="best")
      plt.xticks(ind + WIDTH/2, grp[CCHF_YEAR_COL])
      plt.savefig(f"{PLOTS_DIR}/yearly_cchf_cases_and_deaths_data_for_{country}'s_{key}_district.png")
      plt.clf()

def gen_correlation_matrix_for_data(combined_data: pd.DataFrame, columns_to_comp: list, replace_nas: bool = False):

  removed_nas = ""
  if replace_nas is True:
    combined_data = combined_data.fillna(0)
  else:
    combined_data.dropna(inplace=True)
    removed_nas = "removed_nas"

  countries_in_df = set(combined_data[COUNTRY_PROMED_COL].values)
  
  for country in countries_in_df:
    country_combined_df = combined_data[combined_data[COUNTRY_PROMED_COL] == country]

    districts = set(country_combined_df[CCHF_DISTRICT_COL].values)

    for district in districts:
      district_combined_df = country_combined_df[country_combined_df[CCHF_DISTRICT_COL] == district]
      
      # Grab the data we only care about which is the year, total cases, total deaths and the number of cattle
      fiiltered_df = district_combined_df[columns_to_comp]
      
      try:
        print(fiiltered_df.corr())
        # Generate a visual representation of the Correlation Matrix
        sn.heatmap(fiiltered_df.corr(), annot=True)
        if not os.path.isdir(PLOTS_DIR):
          os.mkdir(PLOTS_DIR)

        file_name = f"{PLOTS_DIR}/{country}_{district}_district_data_correlation_matrix_{removed_nas}.png"
        plt.tight_layout()
        plt.title(f"{country}'s {district} Correlation Matrix")
        plt.savefig(f"{file_name}")
        plt.clf()
      except Exception as err:
        continue

if __name__ == "__main__":
  main()