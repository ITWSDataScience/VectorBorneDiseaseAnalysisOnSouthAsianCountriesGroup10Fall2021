import argparse
import os
import pandas as pd
import re
import spacy
import sys

from datetime import datetime

from geopy.extra.rate_limiter import RateLimiter
from geopy import Nominatim

from epitator.geoname_annotator import GeonameAnnotator
from epitator.date_annotator import DateAnnotator
from epitator.count_annotator import CountAnnotator
from epitator.annotator import AnnoDoc

from typing import Iterable, Union
from transformers import BartForConditionalGeneration, BartTokenizer
from tqdm import tqdm

os.environ['SPACY_MODEL_SHORTCUT_LINK'] = 'en_core_web_trf'

spacy.prefer_gpu()

sys.path.append('../EpiTator')

locator = Nominatim(user_agent="ppcoom")
geocode = RateLimiter(locator.geocode, min_delay_seconds=1/20)

locator = Nominatim(user_agent="ppcoom")
geocode = RateLimiter(locator.geocode, min_delay_seconds=1/20)
dengue_regex = re.compile(
    r'([A-Za-z ]+).*\[w\/e (.+)\] \/ (.+) \/ (.+) \/ (.+) \/ (.+) \/ (.+)', re.MULTILINE)

tqdm.pandas()

# setup our BART transformer summarization model
print('loading transformers')
tokenizer = BartTokenizer.from_pretrained('facebook/bart-large-cnn')
model = BartForConditionalGeneration.from_pretrained(
    'facebook/bart-large-cnn')

COUNTRY_COL = "country"
CONTENT_COL = "content"
SUMMARY_COL = "summary"

DATA_DIR = "../data"
SUMMARIZED_DATA_DIR = f"{DATA_DIR}/summarized"
EXTRACTED_DATA_DIR = f"{DATA_DIR}/extracted"

def extract_arguments() -> Iterable[Union[str, list]]:
  """
  Name: extract_arguments

  Purpose: extracts the arguments specified by the user

  Input: None

  Output: filepath - The csv filepath specified by the user
          countries - The countries specified by the user
  """

  CSV_FILE_ENDING = ".csv"

  parser = argparse.ArgumentParser()
  
  parser.add_argument("-f", "--filepath", type=str, required=True, help="The filepath to the promed data to analyze")
  parser.add_argument("-c", "--countries", nargs="+", required=True, help="The countries to filter for in the data")

  args = parser.parse_args()

  """
  Validate the following:

    1. The filepath has a length > 0
    2. The filepath actually points to a file
    3. The file pointed to by the filepath is a csv
  """

  filepath = args.filepath
  if (
    len(filepath) <= 0 or 
    os.path.isfile(filepath) is False or
    filepath.endswith(CSV_FILE_ENDING) is False
  ):
    print(f"The filepath: {filepath} is either not a valid csv or a valid file.")
    sys.exit(-1)

  """
  Validate the countries specified are valid strings
  """
  invalid_country_specified = False
  for country in args.countries:
    
    if (len(country.strip()) <= 0 or country is None):
      print(f"The country: {country} is not valid")
      invalid_country_specified = True
  
  if invalid_country_specified:
    sys.exit(-1)

  return filepath, args.countries

def read_data(csv_filepath: str) -> pd.DataFrame:
  """
  Name: read_data

  Purpose: To read the data inside the csv filepath specified

  Input: csv_filepath - The filepath to the csv

  Output: A DataFrame representation of the csv data
  """
  return pd.read_csv(csv_filepath)

def filter_df_by_countries(promed_df: pd.DataFrame, countries_to_srch_for: list) -> pd.DataFrame:
  """
  Name: filter_df_by_countries

  Purpose: Filter the specified data frame by the countries specified

  Input: promed_df - The promed dataframe
         countries_to_srch_for - The countries we shoud filter on

  Output: A new filtered dataframe
  """
  filtered_pd = None
  for country in countries_to_srch_for:
    country_filtered_df = promed_df.loc[(promed_df[COUNTRY_COL].str.lower() == country.lower())]
    
    if filtered_pd is None:
      filtered_pd = country_filtered_df
    else:
      filtered_pd.append(country_filtered_df)
  
  return filtered_pd

def clean_df_content(promed_df: pd.DataFrame, debug: bool = False) -> pd.DataFrame:

  cleaned_df = {}

  for index, row in promed_df.iterrows():

    content = row[CONTENT_COL]

    cleaned_content = clean(content)

    if (debug):
      print("---------------------------")
      print(f"{content}")
      print("---------------------------")

    for col in promed_df.columns:

      row_val = row[col]
      if col == CONTENT_COL:
        row_val = cleaned_content

      if col in cleaned_df:
        cleaned_df[col].append(row_val)
      else:
        cleaned_df[col] = [row_val]

  return pd.DataFrame(cleaned_df)

def clean(content):
  split = content.splitlines()
  last_index = -1
  lower = [x.lower().strip() for x in split]
  if '--' in lower:
      last_index = lower.index('--')
  elif 'communicated by:' in lower:
      last_index = lower.index('communicated by:')-1

  cleaned = split[12:last_index]
  return '\n'.join([x for x in cleaned if x])

def summarize_df_content(promed_df: pd.DataFrame) -> pd.DataFrame:
  
  summarized_df = {}

  for index, row in promed_df.iterrows():

    content = row[CONTENT_COL]

    summarized_content = summarizer(content)

    for col in promed_df.columns:

      row_val = row[col]
      if col == SUMMARY_COL:
        row_val = summarized_content

      if col != CONTENT_COL:
        if col in summarized_df:
          summarized_df[col].append(row_val)
        else:
          summarized_df[col] = [row_val]

  return pd.DataFrame(summarized_df)

def summarizer(text: str) -> str:
  input_ids = tokenizer(text, return_tensors='pt', max_length=1024,
                        padding=True, truncation=True)['input_ids']
  summary_ids = model.generate(input_ids)
  summary = ''.join([tokenizer.decode(s) for s in summary_ids])
  summary = summary.replace('<s>', '').replace('</s>', '')
  return summary

def extract_cchf_data_from_df(promed_df: pd.DataFrame) -> pd.DataFrame:

  promed_df[[
    'admin1_code',
    'admin2_code',
    'admin3_code',
    'admin4_code',
    'location_name',
    'location_lat',
    'location_lon',
    'cases',
    'cases_tags',
    'deaths',
    'deaths_tags',
    'dates_start',
    'dates_end',
  ]] = promed_df[SUMMARY_COL].progress_apply(epitator_extract)
  promed_df = promed_df.applymap(lambda x: x[0] if isinstance(
      x, list) and len(x) > 0 else x)
  promed_df = promed_df.applymap(lambda y: pd.NA if isinstance(
      y, (list, str)) and len(y) == 0 else y)
  promed_df = promed_df.reset_index(drop=True)

  return promed_df

# function that extracts location names/admin codes/lat/lng, case and death counts, and date ranges from the input string
# uses epitator since it already trained rules for extracting medical/infectious disease data
def epitator_extract(txt: str, max_ents: int = 1) -> dict:
  # input string and add annotators
  doc = AnnoDoc(txt)
  doc.add_tiers(GeonameAnnotator())
  doc.add_tiers(CountAnnotator())
  doc.add_tiers(DateAnnotator())

  # extract geographic data
  geos = doc.tiers["geonames"].spans
  geo_admin1s = [x.geoname.admin1_code for x in geos]
  geo_admin2s = [x.geoname.admin2_code for x in geos]
  geo_admin3s = [x.geoname.admin3_code for x in geos]
  geo_admin4s = [x.geoname.admin4_code for x in geos]
  geo_names = [x.geoname.name for x in geos]
  geo_lats = [x.geoname.latitude for x in geos]
  geo_lons = [x.geoname.longitude for x in geos]

  # extract case counts and death counts
  counts = doc.tiers["counts"].spans
  cases_counts = [x.metadata['count'] for x in counts if 'case' in x.metadata['attributes']
                  and 'death' not in x.metadata['attributes']]
  cases_tags = [x.metadata['attributes']
                for x in counts if 'case' in x.metadata['attributes'] and 'death' not in x.metadata['attributes']]
  death_counts = [x.metadata['count']
                  for x in counts if 'death' in x.metadata['attributes']]
  death_tags = [x.metadata['attributes']
                for x in counts if 'death' in x.metadata['attributes']]

  # extract the date range
  dates = doc.tiers["dates"].spans
  dates_start = [pd.to_datetime(
      x.metadata["datetime_range"][0], errors='coerce') for x in dates]
  dates_end = [pd.to_datetime(
      x.metadata["datetime_range"][1], errors='coerce') for x in dates]

  # return only max_ents entities from the extracted lists
  # currently set to the first result for each list, since that is usually the most important one
  # and other ones can be filler/garbage data
  return pd.Series([
    geo_admin1s[:max_ents],
    geo_admin2s[:max_ents],
    geo_admin3s[:max_ents],
    geo_admin4s[:max_ents],
    geo_names[:max_ents],
    geo_lats[:max_ents],
    geo_lons[:max_ents],
    cases_counts[:max_ents],
    cases_tags[:max_ents],
    death_counts[:max_ents],
    death_tags[:max_ents],
    dates_start[:max_ents],
    dates_end[:max_ents],
  ])

def main():
  
  print("Extracting the specified arguments")

  csv_filepath, countries = extract_arguments()

  print("Reading the promed data")

  orig_promed_df = read_data(
    csv_filepath = csv_filepath
  )

  print("Filtering the promed data")

  filtered_promed_df = filter_df_by_countries(
    promed_df = orig_promed_df,
    countries_to_srch_for = countries
  )

  print(filtered_promed_df)

  print("Cleaning the promed data")

  cleaned_promed_content_df = clean_df_content(
    promed_df = filtered_promed_df
  )

  print("Summarizing dataframe contents")
  summarized_promed_data = summarize_df_content(
    promed_df = filtered_promed_df
  )
  
  if os.path.isdir(SUMMARIZED_DATA_DIR) is False:
    os.mkdir(SUMMARIZED_DATA_DIR)

  csv_countries_selected = ""
  for country in countries:
    csv_countries_selected += f"_{country.lower()}"

  print("Saving summarized promed data")

  csv_country_summarized_data = f"summarized_promed_cchf_data"
  summarized_promed_data.to_csv(f"{SUMMARIZED_DATA_DIR}/{csv_country_summarized_data}{csv_countries_selected}.csv", index=False)

  print("Extracting promed data")

  extraced_promed_data_df = extract_cchf_data_from_df(
    promed_df = summarized_promed_data
  )

  print("Saving extracted promed data")

  if os.path.isdir(EXTRACTED_DATA_DIR) is False:
    os.mkdir(EXTRACTED_DATA_DIR)
  csv_country_extracted_data = f"extracted_promed_cchf_data"
  extraced_promed_data_df.to_csv(f"{EXTRACTED_DATA_DIR}/{csv_country_extracted_data}{csv_countries_selected}.csv", index=False)

if __name__ == "__main__":
  main()