import sqlite3
import time
import geopandas as gpd
import pandas as pd

from urllib.request import urlopen
from zipfile import ZipFile
from io import BytesIO
from functools import cache

# https://www1.aer.ca/ProductCatalogue/OILS.html

# Connect to SQLite database
conn = sqlite3.connect('your_database.db')
cursor = conn.cursor()

# location of untracked data folders
####### this will break when called from main
folder_location = './files/'


def download_and_unzip(url, extract_to='./'):
    try:
        http_response = urlopen(url)
        zipfile = ZipFile(BytesIO(http_response.read()))
        zipfile.extractall(path=extract_to)
    except:
        raise ValueError('Failed to download and extract files:' + url)

def load_pipeline_data():
    url = 'https://static.aer.ca/prd/data/pipeline/Pipelines_SHP.zip'
    download_and_unzip(url,folder_location)

    pipelines = gpd.read_file((folder_location+'Pipelines_SHP'), driver='FileGDB', layer='Pipelines_GCS_NAD83')
    pd.DataFrame(pipelines).to_sql('pipeline_data_raw',conn,if_exists = 'append', index = 'OBJECTID' )
    
    grouped_gdf = pipelines.dissolve(by='LICENCE_NO')
    pd.DataFrame(grouped_gdf).to_sql('pipeline_data_for_map',conn,if_exists = 'append', index = 'LICENCE_NO' )
    
    pipeline_ocm = pipelines.dissolve(by='BA_CODE')
    # Create a buffer around the polyline

    buffer_distance = 0.1
    pipeline_ocm['buffer_geometry'] = pipeline_ocm['geometry'].buffer(buffer_distance)
    
    pd.DataFrame(pipeline_ocm).to_sql('pipeline_data_for_ocm',conn,if_exists = 'append', index = 'BA_CODE')


def load_bottom_hole_data():
    url = 'https://static.aer.ca/prd/documents/sts/st37/BottomHolesShapefile.zip'
    download_and_unzip(url,folder_location)

    wels_bh = gpd.read_file((folder_location+'ST37_BH'), driver='FileGDB', layer='ST37_BH_GCS_NAD83')
    pd.DataFrame(wels_bh).to_sql('wells_bottomhole',conn,if_exists = 'append', index = 'UWI' )
    
def load_surface_locations_data():
    url = 'https://static.aer.ca/prd/documents/sts/st37/SurfaceHolesShapefile.zip'
    download_and_unzip(url,folder_location)

    surface_wells = gpd.read_file((folder_location+'ST37_SH'), driver='FileGDB', layer='ST37_SH_GCS_NAD83')
    pd.DataFrame(surface_wells).to_sql('wells_surface',conn,if_exists = 'append', index = 'UWI' )
    
def load_facilities_data():
    url = 'https://static.aer.ca/prd/data/codes/ST102-SHP.zip'
    download_and_unzip(url,folder_location)

    facilities = gpd.read_file((folder_location+'ST102-SHP'), driver='FileGDB', layer='ST102_Facility_GCS_NAD83')
    pd.DataFrame(facilities).to_sql('facilities',conn,if_exists = 'append', index = 'FAC_ID' )

def load_field_and_area_boundries():
    # Download field names
    url = 'https://aer.ca/documents/data/FieldCentreRegionalOfficeBoundaryMap.gdb.zip'
    download_and_unzip(url,folder_location)
    # download area names
    url = 'https://static.aer.ca/prd/data/shapefiles/Scheme_Approval_SHP.zip'
    download_and_unzip(url,folder_location)

    fields = gpd.read_file((folder_location+'FieldCentreRegionalOfficeBoundaryMap.gdb'), driver='FileGDB', layer='AER_CIC_Boundaries')
    pd.DataFrame(fields).to_sql('fields',conn,if_exists = 'append', index = 'CIC_Area_Name')
    
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
    pd.DataFrame(areas_combined_gdf).to_sql('area_data_all',conn,if_exists = 'append', index = 'SCHEME_NO')

    areas_grouped = areas_combined_gdf.dissolve(by='PROD_FIELD')
    pd.DataFrame(areas_grouped).to_sql('area_data_all',conn,if_exists = 'append', index = 'PROD_FIELD')

# Function to update database with new data
def update_database(new_data):
    for item in new_data:
        cursor.execute('''
            INSERT OR REPLACE INTO main_table (unique_identifier, attribute1, attribute2)
            VALUES (?, ?, ?)
        ''', (item['unique_identifier'], item['attribute1'], item['attribute2']))
        
        # Log the change
        cursor.execute('''
            INSERT INTO change_log (change_type, unique_identifier, attribute1, attribute2)
            VALUES (?, ?, ?, ?)
        ''', ('insert' if item['operation'] == 'insert' else 'update', item['unique_identifier'], item['attribute1'], item['attribute2']))
    conn.commit()

# Main function to periodically retrieve and update data
def main():
    ...


if __name__ == "__main__":
    main()
