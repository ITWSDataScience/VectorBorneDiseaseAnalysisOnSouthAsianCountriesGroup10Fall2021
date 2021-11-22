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

@app.route('/', methods=["POST","GET"])
def homepage():
    
    cchf_df = pd.read_csv("../data/complete_data.csv")
    vgi_df = pd.read_csv("../data/individual_data_sets/vegetation_data/vgi_data.csv")

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
        geo_data="../data/geodata/afghanistan-serbia-pakistan.geojson",
        name='Afghanistan, Serbia, and Pakistan CCHF Cases',
        data=filtered_data,
        columns=[COUNTRY_COL, TOT_CASES_CCHF_COL],
        key_on='feature.properties.name',
        fill_color='YlOrRd',
        fill_opacity=0.7,
        line_opacity=0.2,
        legend_name='Number of CCHF Cases',
        show=True
    ).add_to(folium_map)
    
    folium.Choropleth(
        geo_data="../data/geodata/afghanistan-serbia-pakistan-districts.geojson",
        name='Afghanistan, Serbia, and Pakistan Average District Vegetation',
        data=vgi_df,
        columns=[DISTRICT_COL, AVG_NVDI_COL],
        key_on='feature.properties.name',
        fill_color='YlGnBu',
        fill_opacity=0.7,
        line_opacity=0.2,
        legend_name='Vegetation Index',
        show=False
    ).add_to(folium_map)

    folium.LayerControl().add_to(folium_map)
    
    # # Iquitos Marker creation
    # iquitos_data = filtered_data[filtered_data[CITIES_COLUMN] == "Iquitos"]
    # coordinates = [-3.7437, -73.2516]
    # create_info_marker(iquitos_data, coordinates, folium_map)
    
    # # San Juan Marker creation
    # san_juan_data = filtered_data[filtered_data[CITIES_COLUMN] == "San Juan"]
    # coordinates = [18.4655, -66.1057]
    # create_info_marker(san_juan_data, coordinates, folium_map)
    
    date_selected = {
        "year_selected"  : session[YEAR_KEY]
    }
    
    folium_map.save('templates/map.html')
    
    return render_template("index.html", data = date_selected)

@app.route('/map')
def show_map():
    return render_template('map.html')

if __name__ == '__main__':
    app.run(debug=False)