import sqlite3
import time
import geopandas as gpd
import pandas as pd

from urllib.request import urlopen
from zipfile import ZipFile
from io import BytesIO
from functools import cache
from os.path import exists
from datetime import datetime,getmtime

# https://www1.aer.ca/ProductCatalogue/OILS.html

# Connect to SQLite database
conn = sqlite3.connect('your_database.db')
cursor = conn.cursor()

# location of untracked data folders
####### this will break when called from main
folder_location = './files/'


def download_and_unzip(url, folder_to_check, extract_to='./'):
    if not exists('data/rng_bin/649.csv') or\
          datetime.utcfromtimestamp(getmtime(folder_to_check)).date() != datetime.now().date():
        try:
            http_response = urlopen(url)
            zipfile = ZipFile(BytesIO(http_response.read()))
            zipfile.extractall(path=extract_to)
        except:
            raise ValueError('Failed to download and extract files:' + url)
    else:
        print("File updated earlier today")

def load_alberta_pipeline_data():
    url = 'https://static.aer.ca/prd/data/pipeline/Pipelines_SHP.zip'
    folder = folder_location+'Pipelines_SHP'
    download_and_unzip(url,folder_location)

    pipelines = gpd.read_file(folder, driver='FileGDB', layer='Pipelines_GCS_NAD83')
    pd.DataFrame(pipelines).to_sql('alberta_pipeline_data_raw',conn,if_exists = 'append', index = 'OBJECTID' )
    
    grouped_gdf = pipelines.dissolve(by='LICENCE_NO')
    grouped_gdf = grouped_gdf['geography']
    pd.DataFrame(grouped_gdf).to_sql('alberta_pipeline_data_for_map',conn,if_exists = 'append', index = 'LICENCE_NO' )
    
    pipeline_ocm = pipelines.dissolve(by='BA_CODE')
    # Create a buffer around the polyline

    buffer_distance = 0.02
    pipeline_ocm['buffer_geometry'] = pipeline_ocm['geometry'].buffer(buffer_distance)
    
    pd.DataFrame(pipeline_ocm).to_sql('alberta_pipeline_data_for_ocm',conn,if_exists = 'append', index = 'BA_CODE')


def load_alberta_bottom_hole_data():
    url = 'https://static.aer.ca/prd/documents/sts/st37/BottomHolesShapefile.zip'
    folder = (folder_location+'ST37_BH')
    download_and_unzip(url,folder,folder_location)

    wels_bh = gpd.read_file(folder, driver='FileGDB', layer='ST37_BH_GCS_NAD83')
    pd.DataFrame(wels_bh).to_sql('alberta_wells_bottomhole',conn,if_exists = 'append', index = 'UWI' )
    

def load_alberta_surface_locations_data():
    url = 'https://static.aer.ca/prd/documents/sts/st37/SurfaceHolesShapefile.zip'
    folder = (folder_location+'ST37_SH')
    download_and_unzip(url,folder,folder_location)

    surface_wells = gpd.read_file(folder, driver='FileGDB', layer='ST37_SH_GCS_NAD83')
    pd.DataFrame(surface_wells).to_sql('alberta_wells_surface',conn,if_exists = 'append', index = 'UWI' )
    

def load_alberta_facilities_data():
    url = 'https://static.aer.ca/prd/data/codes/ST102-SHP.zip'
    folder = (folder_location+'ST102-SHP')
    download_and_unzip(url,folder,folder_location)

    facilities = gpd.read_file(folder, driver='FileGDB', layer='ST102_Facility_GCS_NAD83')
    pd.DataFrame(facilities).to_sql('alberta_facilities',conn,if_exists = 'append', index = 'FAC_ID' )


def load_alberta_field_and_area_boundries():
    # Download field names
    url = 'https://aer.ca/documents/data/FieldCentreRegionalOfficeBoundaryMap.gdb.zip'
    folder = 'FieldCentreRegionalOfficeBoundaryMap.gdb.zip'
    download_and_unzip(url,folder,folder_location)
    # download area names
    url = 'https://static.aer.ca/prd/data/shapefiles/Scheme_Approval_SHP.zip'
    download_and_unzip(url,folder,folder_location)

    fields = gpd.read_file((folder_location+'FieldCentreRegionalOfficeBoundaryMap.gdb'), driver='FileGDB', layer='AER_CIC_Boundaries')
    pd.DataFrame(fields).to_sql('alberta_fields',conn,if_exists = 'append', index = 'CIC_Area_Name')
    
    # get all area data
    area_folders = ['Conventional ER Scheme Approvals', 'In Situ Oil Sands Scheme Approvals', 'Mineable Oil Sands Scheme Approvals', 'Oil Sands Areas', 'PBR Scheme Approvals', 'Surface Mineable Area']
        
    gdf_array = []

    # Iterate through each approval type (area folder)
    for approval_type in area_folders:
        folder_to_read = folder_location+'Scheme_Approval_SHP/Shapefiles'+'/'+approval_type+'_GCS_NAD83'
        # Load data from the area folder
        area_gdf = gpd.read_file(folder_to_read,layer=approval_type+'_GCS_NAD83')
        
        # Append the GeoDataFrame to the array
        gdf_array.append(area_gdf)

    # Concatenate all GeoDataFrames in the array into a single GeoDataFrame
    areas_combined_gdf = gpd.GeoDataFrame(pd.concat(gdf_array, ignore_index=True), crs=gdf_array[0].crs)
    pd.DataFrame(areas_combined_gdf).to_sql('alberta_area_data_all',conn,if_exists = 'append', index = 'SCHEME_NO')

    areas_grouped = areas_combined_gdf.dissolve(by='PROD_FIELD')
    pd.DataFrame(areas_grouped).to_sql('alberta_area_data_all',conn,if_exists = 'append', index = 'PROD_FIELD')



# Main function to periodically retrieve and update data
def main():
    load_alberta_pipeline_data()
    load_alberta_bottom_hole_data()
    load_alberta_surface_locations_data()
    load_alberta_facilities_data()
    load_alberta_field_and_area_boundries()


if __name__ == "__main__":
    main()
