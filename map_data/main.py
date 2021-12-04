# -*- coding: utf-8 -*-
"""
Created on Tue Apr  7 15:55:57 2020
@author: Dominic Schroeder and Karan Bhanot
"""
from flask import Flask, render_template, request, session, redirect
from geopy.geocoders import Nominatim
import folium
import pandas as pd
import json
import math

app = Flask(__name__)
# Required in order to use session cookies
app.secret_key = "super secret key"

# Required so the map html template is updated automatically
app.config['TEMPLATES_AUTO_RELOAD'] = True

# Required so Flask knows where to fetch the images
#app.add_url_rule('/images/<path:filename>', endpoint='images', view_func=app.send_static_file)

COMBINED_DATA = "../data/combined_district_data.csv"
CCHF_DISTRICT_DATA = "../data/individual_data_sets/CCHF_data/cchf_district_data.csv"

# Constants for the data
COUNTRY_COL = "country"
DISEASE_COL = "diseasename"
YEAR_COL = "year"
TOT_CASES_CCHF_COL = "total cases"
TOT_DEATHS_CCHF_COL = "total deaths"
NUM_OF_CATTLE_COL = "Num of cattle"
POPULATION_COL = "Population"

YEAR_KEY = "year"

DISTRICT_COL = "district"
AVG_NVDI_COL = "Avg. NVDI Val"
COUNTRY_PRECIPITATION_COL = "PRECTOTLAND kg m-2 s-1"
COUNTRY_TEMPERATURE_COL = "temperature in (K)"
DISTRICT_LAT_COL = "region/city lat" 
DISTRICT_LON_COL = "region/city lon"

@app.route('/', methods=["POST","GET"])
def homepage():
    
  cchf_df = pd.read_csv(COMBINED_DATA)

  # Default to the first item in our select list
  if (YEAR_KEY not in session):
      session[YEAR_KEY] = 1995
  
  # Persist the values selected by the user in their session
  if request.method == 'POST':
      session.clear()
      session[YEAR_KEY] = int(request.form[YEAR_KEY])

  # Convert the month and year to the proper format for parsing the dataframe
  filtered_data = cchf_df[cchf_df[YEAR_COL] == session[YEAR_KEY]]
  
  # Set the coordinates and zoom so we can see both points
  start_coords = (34.00, 63.00)
  folium_map = folium.Map(location=start_coords, zoom_start=4)

  # Add a map layer to allow for a heat map using the GeoJSON we created
  folium.Choropleth(
      geo_data="../data/geodata/afghanistan-serbia-pakistan-districts.geojson",
      name='Afghanistan, Serbia, and Pakistan CCHF Cases',
      data=filtered_data,
      columns=[DISTRICT_COL, TOT_CASES_CCHF_COL],
      key_on='feature.properties.name',
      fill_color='YlOrRd',
      fill_opacity=0.7,
      line_opacity=0.2,
      legend_name='Number of CCHF Cases',
      show=True
  ).add_to(folium_map)
  
  create_info_markers(
    data = filtered_data,
    folium_map = folium_map
  )
  
  date_selected = {
      "year_selected"  : session[YEAR_KEY]
  }
  
  folium_map.save('templates/map.html')
  
  return render_template("index.html", data = date_selected)

@app.route('/map')
def show_map():
    return render_template('map.html')

def create_info_markers(data, folium_map):
    
  marked_districts = []

  district_coords_df = create_district_coords_map()

  # In case a user decides to select a year we dont have data for
  for district, coords_data in district_coords_df.items():
  
    if data.empty or data[data[DISTRICT_COL] == district].empty:
      
      coordinates = [coords_data[DISTRICT_LAT_COL], coords_data[DISTRICT_LON_COL]]
      folium.Marker(
        location = coordinates,
        popup = "No data found for this location.",
        icon = folium.Icon(color='gray')
      ).add_to(folium_map)

      marked_districts.append(district)

  if data.empty:
    return

  for idx, row in data.iterrows():

    district = row[DISTRICT_COL]

    if district in marked_districts:
      continue

    marked_districts.append(district)

    coordinates = [row[DISTRICT_LAT_COL], row[DISTRICT_LON_COL]]

    # In case we don't have data for a specific column
    num_of_cchf_cases = "Unknown"
    num_of_cchf_deaths = "Unknown"
    surface_temperature = "Unknown"
    precipitation = "Unknown"
    nvdi = "Unknown"
    
    cchf_cases_row_val = row[TOT_CASES_CCHF_COL]
    cchf_deaths_row_val = row[TOT_DEATHS_CCHF_COL]
    surface_temp_row_val = row[COUNTRY_TEMPERATURE_COL]
    precip_row_val = row[COUNTRY_PRECIPITATION_COL]
    nvdi_row_val = row[AVG_NVDI_COL]

    if math.isnan(cchf_cases_row_val) is False:
      num_of_cchf_cases = cchf_cases_row_val

    if math.isnan(cchf_deaths_row_val) is False:
      num_of_cchf_deaths = cchf_deaths_row_val

    if math.isnan(surface_temp_row_val) is False:
      surface_temperature = surface_temp_row_val

    if math.isnan(precip_row_val) is False:
      precipitation = precip_row_val

    if math.isnan(nvdi_row_val) is False:
      nvdi = nvdi_row_val

    
    info_message = f"""Total Number of CCHF Cases: {num_of_cchf_cases} <br>
                       Total Number of CCHF Deaths: {num_of_cchf_deaths} <br>
                       Avg Surface Temp: {surface_temperature} K<br>
                       Avg Precipitation: {precipitation} kg m-2 s-1<br>
                       Avg NVDI Value: {nvdi} <br>
                    """ 

    folium.Marker(location = coordinates,
                     popup = folium.Popup(info_message, max_width=250,min_width=250),
                     icon = folium.Icon(color='blue')
                 ).add_to(folium_map)

def create_district_coords_map(filepath: str = CCHF_DISTRICT_DATA):
  
  cchf_df = pd.read_csv(filepath)

  district_coords_df = cchf_df[[DISTRICT_COL, DISTRICT_LAT_COL, DISTRICT_LON_COL]]

  district_coords_map = {}
  for idx, row in district_coords_df.iterrows():
    district = row[DISTRICT_COL]
    district_lat = row[DISTRICT_LAT_COL]
    district_lon = row[DISTRICT_LON_COL]

    if district not in district_coords_map and math.isnan(district_lat) is False and math.isnan(district_lon) is False:
      district_coords_map[district] = {
        DISTRICT_LAT_COL : district_lat,
        DISTRICT_LON_COL : district_lon
      }

  return district_coords_map

if __name__ == '__main__':
    app.run(debug=False)