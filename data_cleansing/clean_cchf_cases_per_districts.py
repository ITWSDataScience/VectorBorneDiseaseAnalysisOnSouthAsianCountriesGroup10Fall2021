import argparse
import os
import sys
import pandas as pd
import json
import math

from typing import Iterable, Union
from shapely.geometry import shape, Point

"""
Original column names for the extracted and validated CCHF data
"""
DISEASE_NAME_COL = "diseasename"
CCHF_PLACE_COL = "place"
CCHF_COUNTRY_COL = "country"
CCHF_COUNTRY_LAT_COL = "lat"
CCHF_COUNTRY_LON_COL = "lon"
CCHF_SUMMARY_COL = "summary"
CCHF_ISSUE_DATE_COL = "issue_date"
CCHF_REG_CITY_COL = "region/city"
CCHF_REG_CITY_LAT_COL = "region/city lat"
CCHF_REG_CITY_LON_COL = "region/city lon"
CCHF_NUM_OF_CASES_COL = "cases"
CCHF_NUM_OF_DEATHS_COL = "deaths" 
CCHF_NUM_OF_TOT_CASES_COL = "total cases"	
CCHF_NUM_OF_TOT_DEATHS_COL = "total deaths"

CCHF_DISTRICT_COL = "district"

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

def main():
  
  csv_datapath = extract_arguments()

  cchf_df = read_data(filepath = csv_datapath)

  cchf_df = correlate_cchf_cases_with_district(extracted_cchf_data=cchf_df)

  cchf_df.to_csv("../data/individual_data_sets/CCHF_data/cchf_district_data.csv", index=False)

def extract_arguments() -> str:
  """
  Purpose: extracts the arguments specified by the user

  Input: None

  Output: filepath - The csv filepath specified by the user
  """

  CSV_FILE_ENDING = ".csv"

  parser = argparse.ArgumentParser()
  
  parser.add_argument("-f", "--filepath", type=str, required=True, help="The filepath to the text file containing the links to pull the files from")

  args = parser.parse_args()

  """
  Validate the following:

    1. The filepath has a length > 0
    2. The filepath actually points to a file
  """

  filepath = args.filepath

  if (
    len(filepath) <= 0 or 
    os.path.isfile(filepath) is False or 
    CSV_FILE_ENDING not in filepath
  ):
    print(f"The filepath: {filepath} is either not a valid file or is not a csv.")
    sys.exit(-1)

  return filepath

def read_data(filepath: str) -> pd.DataFrame:

  return pd.read_csv(filepath)

def correlate_cchf_cases_with_district(extracted_cchf_data: pd.DataFrame) -> pd.DataFrame:

  FEATURES_KEY = "features"
  GEOMETRY_KEY = "geometry"
  PROPERTIES_KEY = "properties"
  NAME_KEY = "name"

  districts = [""] * extracted_cchf_data.shape[0]

  for cchf_idx, row in extracted_cchf_data.iterrows():

    COUNTRY_LOWERCASE = row[CCHF_COUNTRY_COL].lower()
    GEOJSON_DATA = f"../data/geodata/{COUNTRY_LOWERCASE}/{COUNTRY_LOWERCASE}-districts.geojson"

    geodata = None
    with open(GEOJSON_DATA, "r") as geo_file:
      geodata = json.load(geo_file)

    if COUNTRY_LOWERCASE not in COORDS_RANGE:
      print(f"Cannot fetch coordinate info from internal database for {COUNTRY_LOWERCASE}")
      sys.exit(-1)

    min_lat = COORDS_RANGE[COUNTRY_LOWERCASE][MIN_LAT_KEY]
    max_lat = COORDS_RANGE[COUNTRY_LOWERCASE][MAX_LAT_KEY]
    min_lon = COORDS_RANGE[COUNTRY_LOWERCASE][MIN_LON_KEY]
    max_lon = COORDS_RANGE[COUNTRY_LOWERCASE][MAX_LON_KEY]

    lat = row[CCHF_REG_CITY_LAT_COL]
    lon = row[CCHF_REG_CITY_LON_COL]

    if math.isnan(lat) or math.isnan(lon):
      districts[cchf_idx] = ("")
    else:
      coordinate = Point(lon, lat)

      polygon = None
      if FEATURES_KEY in geodata:
        for feature in geodata[FEATURES_KEY]:
          properties = feature[PROPERTIES_KEY]
          feature_name = properties[NAME_KEY]
          polygon = shape(feature[GEOMETRY_KEY])
          if polygon.contains(coordinate):
            districts[cchf_idx] = feature_name

      elif GEOMETRY_KEY in geodata:
        properties = geodata[PROPERTIES_KEY]
        feature_name = properties[NAME_KEY]
        polygon = shape(geodata[GEOMETRY_KEY])
        if polygon.contains(coordinate):
          districts[cchf_idx] = feature_name
        
  extracted_cchf_data.reset_index(inplace=True)
  extracted_cchf_data[CCHF_DISTRICT_COL] = districts

  return extracted_cchf_data

if __name__ == "__main__":
  main()