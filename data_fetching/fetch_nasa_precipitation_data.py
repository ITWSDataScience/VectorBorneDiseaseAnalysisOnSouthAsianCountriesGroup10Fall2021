import argparse
import json
import os
import pandas as pd
import requests
import sys
import numpy as np
import time
import netCDF4 as nc

from pyhdf.SD import SD, SDC
from typing import Iterable, Union
from io import StringIO
from shapely.geometry import shape, Point

MIN_LAT_KEY = "min_lat"
MAX_LAT_KEY = "max_lat"
MIN_LON_KEY = "min_lon"
MAX_LON_KEY = "max_lon"

COORDS_RANGE = {
  "serbia" : {
    MIN_LAT_KEY : 40,
    MAX_LAT_KEY : 48,
    MIN_LON_KEY : 18,
    MAX_LON_KEY : 25
  },
  "pakistan" : {
    MIN_LAT_KEY : 22,
    MAX_LAT_KEY : 38,
    MIN_LON_KEY : 60,
    MAX_LON_KEY : 77
  },
  "afghanistan" : {
    MIN_LAT_KEY : 29,
    MAX_LAT_KEY : 39,
    MIN_LON_KEY : 63,
    MAX_LON_KEY : 70
  }
}

# Temperature Mapping Keys
LAT_KEY = "latitude"
LON_KEY = "longitude"
DISTRICT_KEY = "district"
COUNTRY_KEY = "country"
YEAR_KEY = "year"
MONTH_KEY = "month"
PRECP_TOT_KEY = "PRECTOTLAND kg m-2 s-1"

"""
Notes:

Before running this python script it is required to have the appropriate setup in order to
execute wget properly to rechieve the NASA data. Please see this link for the setup steps 
required: https://disc.gsfc.nasa.gov/data-access#windows_wget
"""

def main():
  
  nasa_links_filepath, countries, download_data = extract_arguments()
  fileinfos = retrieve_nasa_data(
    filepath = nasa_links_filepath,
    download_data = download_data
  )

  precipitation_data_map = retrieve_precipitation_data(fileinfos=fileinfos)
  print("Finished downloading and parsing NASA data")

  print("Collapsing temperature data map")
  precipitation_df = collapse_precipitation_data(
    temp_data_map = precipitation_data_map,
    countries = countries
  )
  print("Finished collapsing temperature data map")

  yearly_precipitation_district_avg_df = combine_data_to_be_yearly_average_per_district(
    countries = countries,
    df = precipitation_df
  )
  yearly_precipitation_district_avg_df.to_csv("yearly_precipitation_data_by_district.csv", index = False)

def extract_arguments() -> Iterable[Union[str, list, bool]]:
  """
  Purpose: extracts the arguments specified by the user

  Input: None

  Output: filepath - The csv filepath specified by the user
  """

  CSV_FILE_ENDING = ".csv"

  parser = argparse.ArgumentParser()
  
  parser.add_argument("-f", "--filepath", type=str, required=True, help="The filepath to the text file containing the links to pull the files from")
  parser.add_argument("-d", "--download", required=False, action='store_true', help="Fetch all of the data specified in the file")
  parser.add_argument("-c", "--countries", type=str, nargs="+", required=True, help="The countries we wish to fetch the NDVI data for")

  args = parser.parse_args()

  """
  Validate the following:

    1. The filepath has a length > 0
    2. The filepath actually points to a file
    3. The file pointed to by the filepath is a csv
  """

  filepath = args.filepath
  countries = args.countries
  download_data = args.download

  if (
    len(filepath) <= 0 or 
    os.path.isfile(filepath) is False
  ):
    print(f"The filepath: {filepath} is either not a valid file.")
    sys.exit(-1)

  for country in countries:
    if len(country) <= 0:
      print(f"The country: {country} is not valid")
      sys.exit(-1)

  return filepath, countries, download_data

def retrieve_nasa_data(filepath: str, download_data: bool) -> list:
  """
  Purpose: Issues wget calls and downloads the specified data files from the urls stored in the
  specified text files

  Input: filepath - The filepth to the file holding the links

  Output: A list of all the files downloaded from the 
  """
  SKIP_FILE_FLAG = "#"

  fileinfos = []
  links = []
  
  with open(filepath, "r") as file_containing_links:
    links = file_containing_links.readlines()

    if download_data:
      for link in links:
        os.system(f"wget --tries=0 --read-timeout=20 --load-cookies ~/.urs_cookies --save-cookies ~/.urs_cookies --auth-no-challenge=on --keep-session-cookies {link.strip()}")
        time.sleep(5)

  for link in links:
    if link[0] == SKIP_FILE_FLAG:
      continue
    file_link_path = link.split("/")
    queried_file_name = file_link_path[-1].strip()
    file_name = queried_file_name.split(".nc4?")[0]
    
    date_recorded_info_start_idx = queried_file_name.find("_Nx.") + len("_Nx.")
    date_recorded_info_end_idx = queried_file_name.find(".nc4")
    date_recorded_info = queried_file_name[date_recorded_info_start_idx:date_recorded_info_end_idx]
    date_recorded_info = f"{date_recorded_info[0:4]}-{date_recorded_info[4:6]}" 

    fileinfos.append((file_name, date_recorded_info))

  for file_name, date_recorded_info in fileinfos:
    for root, dirs, files in os.walk(".", topdown=False):
      for file in files:
        if file_name in file:
          os.rename(file, file_name)

  return fileinfos

def retrieve_precipitation_data(fileinfos: list) -> dict:

  precipitation_data = {}

  for file_name, recorded_date in fileinfos:
    try:
      if os.path.isfile(file_name) is False:
        continue
      ds = nc.Dataset(file_name)
      tlml_lats = ds['lat'][:]
      tlml_lons = ds["lon"][:]
      
      precip_data = ds['PRECTOTCORR'][:]
      z, num_lats, num_lons = np.shape(precip_data)

      date_split = recorded_date.split("-")
      year = date_split[0]
      month = date_split[1]

      if year not in precipitation_data:
        precipitation_data[year] = {}
      
      if month not in precipitation_data[year]:
        precipitation_data[year][month] = []

      for z_idx in range(0, z):
        for lat_idx in range(0, num_lats):
          for lon_idx in range(0, num_lons):
            precipitation = precip_data[z_idx, lat_idx, lon_idx]

            precipitation_data[year][month].append({
              LAT_KEY  : tlml_lats[lat_idx],
              LON_KEY  : tlml_lons[lon_idx],
              PRECP_TOT_KEY : precipitation
            })
    except Exception as err:
      pass

  return precipitation_data

def collapse_precipitation_data(temp_data_map: dict, countries: list) -> pd.DataFrame:

  FEATURES_KEY = "features"
  GEOMETRY_KEY = "geometry"
  PROPERTIES_KEY = "properties"
  NAME_KEY = "name"

  precipitation_data = {
    COUNTRY_KEY : [],
    YEAR_KEY : [],
    MONTH_KEY : [],
    DISTRICT_KEY : [],
    LAT_KEY : [],
    LON_KEY : [],
    PRECP_TOT_KEY : []
  }

  for country_to_retrieve in countries:

    COUNTRY_LOWERCASE = country_to_retrieve.lower()
    GEOJSON_DATA = f"../data/geodata/{COUNTRY_LOWERCASE}/{COUNTRY_LOWERCASE}-districts.geojson"

    geodata = None
    with open(GEOJSON_DATA, "r") as geo_file:
      geodata = json.load(geo_file)

    if COUNTRY_LOWERCASE not in COORDS_RANGE:
      print(f"Cannot fetch coordinate info from internal database for {country_to_retrieve}")
      sys.exit(-1)

    min_lat = COORDS_RANGE[COUNTRY_LOWERCASE][MIN_LAT_KEY]
    max_lat = COORDS_RANGE[COUNTRY_LOWERCASE][MAX_LAT_KEY]
    min_lon = COORDS_RANGE[COUNTRY_LOWERCASE][MIN_LON_KEY]
    max_lon = COORDS_RANGE[COUNTRY_LOWERCASE][MAX_LON_KEY]

    years = temp_data_map.keys()
    for year in years:
      months = temp_data_map[year].keys()
      for month in months:
        list_of_temp_data = temp_data_map[year][month]

        for temp_info in list_of_temp_data:
          lat = temp_info[LAT_KEY]
          lon = temp_info[LON_KEY]
          temp = temp_info[PRECP_TOT_KEY]

          if max_lat > lat and lat > min_lat and max_lon > lon and lon > min_lon:
            coordinate = Point(lon, lat)

            polygon = None
            if FEATURES_KEY in geodata:
              for feature in geodata[FEATURES_KEY]:
                properties = feature[PROPERTIES_KEY]
                feature_name = properties[NAME_KEY]
                polygon = shape(feature[GEOMETRY_KEY])
                if polygon.contains(coordinate):
                  precipitation_data[COUNTRY_KEY].append(country_to_retrieve)
                  precipitation_data[DISTRICT_KEY].append(feature_name)
                  precipitation_data[MONTH_KEY].append(month)
                  precipitation_data[YEAR_KEY].append(year)
                  precipitation_data[LAT_KEY].append(lat)
                  precipitation_data[LON_KEY].append(lon)
                  precipitation_data[PRECP_TOT_KEY].append(temp)

            elif GEOMETRY_KEY in geodata:
              properties = geodata[PROPERTIES_KEY]
              feature_name = properties[NAME_KEY]
              polygon = shape(geodata[GEOMETRY_KEY])
              if polygon.contains(coordinate):
                precipitation_data[COUNTRY_KEY].append(country_to_retrieve)
                precipitation_data[DISTRICT_KEY].append(feature_name)
                precipitation_data[MONTH_KEY].append(month)
                precipitation_data[YEAR_KEY].append(year)
                precipitation_data[LAT_KEY].append(lat)
                precipitation_data[LON_KEY].append(lon)
                precipitation_data[PRECP_TOT_KEY].append(temp)

  precip_df = pd.DataFrame(precipitation_data)

  precip_df_save_name = f"precipitation_data"
  for country_to_retrieve in countries:
    precip_df_save_name += f"_{country_to_retrieve.lower()}"
  precip_df_save_name += ".csv"

  precip_df.to_csv(precip_df_save_name, index=False)

  return precip_df

def combine_data_to_be_yearly_average_per_district(countries: list, df: pd.DataFrame):

  """
  Steps:
    1. Filter on each country
    2. Filter on each year
    3. Filter on each district
    4. Compute the average of each district in that year
  """

  yearly_district_avg_df = {
    COUNTRY_KEY : [],
    DISTRICT_KEY : [],
    YEAR_KEY : [],
    PRECP_TOT_KEY : []
  }

  for country in countries:

    country_df = df[df[COUNTRY_KEY] == country]
    # Now we need to grab a list of unique values for every year
    years = set(country_df[YEAR_KEY].values)

    for year in years:
      # Now we shall filter off of the districts
      yearly_country_df = country_df[country_df[YEAR_KEY] == year]
      
      # We need to get all of the districts
      districts = set(yearly_country_df[DISTRICT_KEY].values)

      for district in districts:        
        precipitation_average = 0
        precipitation_sum = 0

        district_df = yearly_country_df[yearly_country_df[DISTRICT_KEY] == district]

        for idx, row in district_df.iterrows():
          precipitation_sum += row[PRECP_TOT_KEY]
        
        precipitation_average = precipitation_sum/len(district_df.index)

        yearly_district_avg_df[COUNTRY_KEY].append(country)
        yearly_district_avg_df[DISTRICT_KEY].append(district)
        yearly_district_avg_df[YEAR_KEY].append(year)
        yearly_district_avg_df[PRECP_TOT_KEY].append(precipitation_average)
    
  return pd.DataFrame(yearly_district_avg_df)

if __name__ == "__main__":
  main()