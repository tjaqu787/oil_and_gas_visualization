import geopandas as gpd
import pandas as pd
import folium
import numpy as np

path_to_data = 'data/'

def make_map():
    map = folium.Map(location=[53.9333, -116.5765], zoom_start=10)
    map = add_pipeline_layer(map)
    map.save('map.html')

def add_pipeline_layer(map):
    ### need to make this pull from bd to make it faster
    pipelines = gpd.read_file((path_to_data+'Pipelines_SHP'), driver='FileGDB', layer='Pipelines_GCS_NAD83')
    print(pipelines)
    print(pd.DataFrame(pipelines).columns.name)
    folium.GeoJson(pipelines).add_to(map)
    folium.LayerControl(name="Pipelines", control =True,show=False,overlay=False).add_to(map)
    return map

def add_bottom_hole_layer():
    ...

def add_surface_locations_layer():
    ...

def add_facilities_layer():
    ...

if __name__ == '__main__':
    make_map()

