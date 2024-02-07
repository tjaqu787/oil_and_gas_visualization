import geopandas as gpd
import pandas as pd
import folium
import numpy as np
import sqlite3

from data.update_database import main

path_to_data = 'data/'
try:
    conn = sqlite3.connect(path_to_data)
except sqlite3.OperationalError:
    main()

crs = gpd.read_file((path_to_data+'files/'+'FieldCentreRegionalOfficeBoundaryMap.gdb'), driver='FileGDB', layer='AER_CIC_Boundaries').crs

def make_pipeline_map():
    map = folium.Map(location=[53.9333, -116.5765], zoom_start=10)
    map = add_pipeline_layer(map)
    map = add_facilities_layer(map)
    map.save('map.html')

def add_pipeline_layer(map):
    ### need to make this pull from bd to make it faster
    pipelines = gpd.GeoDataFrame(pd.read_sql('select * from alberta_pipeline_data_for_map',conn,))

    folium.GeoJson(pipelines).add_to(map)
    folium.LayerControl(name="Pipelines", control =True,show=False,overlay=False).add_to(map)
    return map

def add_facilities_layer():
    pipelines = gpd.GeoDataFrame(pd.read_sql('select * from facilities',conn,))

    folium.GeoJson(pipelines).add_to(map)
    folium.LayerControl(name="alberta_facilities", control =True,show=True,overlay=True).add_to(map)
    return map

def make_well_map():
    ...

def add_bottom_hole_layers():
    ...

def add_surface_locations_layer():
    ...



if __name__ == '__main__':
    make_pipeline_map()

conn.close()
