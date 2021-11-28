import argparse
import json
import os
import pandas as pd
import requests
import sys
import numpy as np
import time

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

# NVDI Mapping Keys
REC_DATE_KEY = "recorded_date"
LAT_KEY = "latitude"
LON_KEY = "longitude"
NVDI_KEY = "NVDI Val"
DISTRICT_KEY = "district"
COUNTRY_KEY = "country"
YEAR_KEY = "year"
MONTH_KEY = "month"
AVG_NVDI_KEY = "Avg. NVDI Val"

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

  vegetation_index_map = retrieve_country_vegetation_index(fileinfos=fileinfos, countries=countries)

  vgi_df = collapse_VGI_map_to_df(vegetation_index_map=vegetation_index_map)

  yearly_avg_district_df = combine_data_to_be_yearly_average_per_district(
    countries = countries,
    df = vgi_df
  )

  yearly_avg_district_df.to_csv(f"../data/individual_data_sets/vegetation_data/vgi_data.csv", index=False)

def extract_arguments() -> Iterable[Union[str, list, bool]]:
  """
  Purpose: extracts the arguments specified by the user

  Input: None

  Output: filepath - The csv filepath specified by the user
  """

  CSV_FILE_ENDING = ".csv"

  parser = argparse.ArgumentParser()
  
  parser.add_argument("-f", "--filepath", type=str, required=True, help="The filepath to the text file containing the links to pull the files from")
  parser.add_argument("-c", "--countries", type=str, nargs="+", required=True, help="The countries we wish to fetch the NDVI data for")
  parser.add_argument("-d", "--download", type=bool, required=False, default=False, help="Fetch all of the data specified in the file")

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
  fileinfos = []
  links = []
  
  with open(filepath, "r") as file_containing_links:
    links = file_containing_links.readlines()

  for link in links:
    file_link_path = link.split("/")
    file_name = file_link_path[-1].strip()
    date_recorded_info = file_link_path[-2]
    fileinfos.append((file_name, date_recorded_info))

  if download_data:
    for link in links:
      os.system(f"wget --tries=0 --read-timeout=20 --load-cookies ~/.urs_cookies --save-cookies ~/.urs_cookies --auth-no-challenge=on --keep-session-cookies {link.strip()}")
      time.sleep(5)
  
  return fileinfos

def display_hdf_files(filenames: list) -> None:
  """
  Purpose: Displays the contents of the HDF files in the filenames

  Input: A list of HDF filenames

  Output: None   
  """
  for filename in filenames:
    if os.path.isfile(filename):
      file = SD(filename, SDC.READ)
      datasets_dic = file.datasets()

      sds_obj = file.select('CMG 0.05 Deg MONTHLY NDVI') # select sds

      data = sds_obj.get() # get sds data
      print(data.shape)

def retrieve_country_vegetation_index(fileinfos: list, countries: list) -> dict:
  """
  Purpose: Determines which coordinates are inside the bounding box of the
  countries of interest. After determining the coordinates it retrieves the data
  information at that coordinate and stores it into a dictionary

  Input: fileinfos - The file infos list containing tuples of the (filename, date recorded)
         countries - A list of countries 

  Output: A dictionary containing the information on the data of interest
  """
  FEATURES_KEY = "features"
  GEOMETRY_KEY = "geometry"
  PROPERTIES_KEY = "properties"
  NAME_KEY = "name"
  TYPE_KEY = "type"
  COORDINATES_KEY = "coordinates"

  print(f"Starting to get NVDI data")

  vegetation_index_map = {}

  for country_to_retrieve in countries:

    COUNTRY_LOWERCASE = country_to_retrieve.lower()
    GEOJSON_DATA = f"../data/geodata/{COUNTRY_LOWERCASE}/{COUNTRY_LOWERCASE}-districts.geojson"

    geodata = None
    with open(GEOJSON_DATA, "r") as geo_file:
      geodata = json.load(geo_file)

    if COUNTRY_LOWERCASE not in COORDS_RANGE:
      print(f"Cannot fetch coordinate info from internal database for {country_to_retrieve}")
      sys.exit(-1)

    vegetation_index_map[country_to_retrieve] = []

    min_lat = COORDS_RANGE[COUNTRY_LOWERCASE][MIN_LAT_KEY]
    max_lat = COORDS_RANGE[COUNTRY_LOWERCASE][MAX_LAT_KEY]
    min_lon = COORDS_RANGE[COUNTRY_LOWERCASE][MIN_LON_KEY]
    max_lon = COORDS_RANGE[COUNTRY_LOWERCASE][MAX_LON_KEY]

    data = None
    for filename, date_recorded_info in fileinfos:
      vgi_data_retrieval_start = time.time()
      if os.path.isfile(filename):
        
        file = SD(filename, SDC.READ)

        sds_obj = file.select('CMG 0.05 Deg MONTHLY NDVI') # select sds

        data = sds_obj.get() # get sds data

      if data is None:
        continue

      print(f"Analyzing file: {filename}")

      num_rows, num_cols = data.shape
      
      for col_idx in range(0, num_cols):
        for row_idx in range(0, num_rows):
          lon = (col_idx*.05) - 180
          lat = 90-(row_idx*.05)
          if max_lat > lat and lat > min_lat and max_lon > lon and lon > min_lon:
            coordinate = Point(lon, lat)

            polygon = None
            if FEATURES_KEY in geodata:
              for feature in geodata[FEATURES_KEY]:
                properties = feature[PROPERTIES_KEY]
                feature_name = properties[NAME_KEY]
                polygon = shape(feature[GEOMETRY_KEY])
                if polygon.contains(coordinate):

                  vegetation_index_map[country_to_retrieve].append(
                    {
                      DISTRICT_KEY : feature_name,
                      REC_DATE_KEY : date_recorded_info,
                      LAT_KEY : lat,
                      LON_KEY : lon,
                      NVDI_KEY : data[row_idx][col_idx]
                    }
                  )

            elif GEOMETRY_KEY in geodata:
              properties = geodata[PROPERTIES_KEY]
              feature_name = properties[NAME_KEY]
              polygon = shape(geodata[GEOMETRY_KEY])
              if polygon.contains(coordinate):
                vegetation_index_map[country_to_retrieve].append(
                  {
                    DISTRICT_KEY : feature_name,
                    REC_DATE_KEY : date_recorded_info,
                    LAT_KEY : lat,
                    LON_KEY : lon,
                    NVDI_KEY : data[row_idx][col_idx]
                  }
                )
      
      print(f"Finished retrieving NVDI data time to complete: : {time.time() - vgi_data_retrieval_start}")

  return vegetation_index_map

def convert_latitude_to_matrix_idx(latitude: int) -> int:
  """
  Purpose: Some HDFs have data stored in a 3600 x 7200 matrix. As a result,
  you need the latitude to be converted into an index value. This is more
  efficient than converting every index in the matrix to determine if it
  in the bounding box.

  Input: latitude - The latitude to convert

  Output: The corresponding index of the matrix
  """
  return int(((90 - latitude)/.05))

def convert_longitude_to_matrix_idx(longitude: int) -> int:
  """
  Purpose: Some HDFs have data stored in a 3600 x 7200 matrix. As a result,
  you need the longitude to be converted into an index value. This is more
  efficient than converting every index in the matrix to determine if it
  in the bounding box.

  Input: longitude - The longitude to convert

  Output: The corresponding index of the matrix
  """
  return int((longitude + 180)/.05)

def collapse_VGI_map_to_df(vegetation_index_map: dict) -> pd.DataFrame:

  """
  Purpose: Collapse the dictionary of data into a CSV containing the
  following columns:

  country, year, month, district, latitude, longitude, NVDI Val
  
  Input: vegetation_index_map - The vegetation data map

  Output: A dataframe containing the collapsed map

  Side-Effects: Saves the csv data in the current directory for analysis
  """

  csv_title = f"vgi_data_for"

  vgi_df = {
    COUNTRY_KEY : [],
    YEAR_KEY : [],
    MONTH_KEY : [],
    DISTRICT_KEY : [],
    LAT_KEY : [],
    LON_KEY : [],
    NVDI_KEY : []
  }

  for country, data in vegetation_index_map.items():

    csv_title += f"_{country}"

    for disctrict_data in data:
      vgi_df[COUNTRY_KEY].append(country)
      vgi_df[DISTRICT_KEY].append(disctrict_data[DISTRICT_KEY])
      recorded_date_split = disctrict_data[REC_DATE_KEY].split(".")
      year = recorded_date_split[0]
      month = recorded_date_split[1]

      vgi_df[YEAR_KEY].append(year)
      vgi_df[MONTH_KEY].append(month)
      vgi_df[LAT_KEY].append(disctrict_data[LAT_KEY])
      vgi_df[LON_KEY].append(disctrict_data[LON_KEY])
      vgi_df[NVDI_KEY].append(disctrict_data[NVDI_KEY])
  
  csv_title += ".csv"
  vgi_df = pd.DataFrame(vgi_df)

  vgi_df.to_csv(csv_title, index = False)

  return vgi_df

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
    AVG_NVDI_KEY : []
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
        nvdi_average = 0
        nvdi_sum = 0

        district_df = yearly_country_df[yearly_country_df[DISTRICT_KEY] == district]

        for idx, row in district_df.iterrows():
          nvdi_val = row[AVG_NVDI_KEY]

          if nvdi_val > -12000:
            nvdi_sum += nvdi_val
        
        nvdi_average = nvdi_sum/len(district_df.index)

        yearly_district_avg_df[COUNTRY_KEY].append(country)
        yearly_district_avg_df[DISTRICT_KEY].append(district)
        yearly_district_avg_df[YEAR_KEY].append(year)
        yearly_district_avg_df[AVG_NVDI_KEY].append(nvdi_average)
    
  return pd.DataFrame(yearly_district_avg_df)

if __name__ == "__main__":
  main()