
import os
import time
import re
from datetime import date, timedelta
from tqdm import tqdm
import pygrib
import pandas as pd
import geopandas as gpd
import numpy as np
import xarray as xr
from shapely.geometry import mapping, Polygon, Point
from rasterio.plot import show
from rasterio import features
import rasterio
from rasterio.mask import mask
from osgeo import gdal
from rasterio.enums import Resampling
from rasterio.warp import reproject
import matplotlib.pyplot as plt

from scipy.interpolate import griddata
from mpl_toolkits.basemap import Basemap
from matplotlib.backends.backend_pdf import PdfPages

import gzip
from ftplib import FTP
import cdsapi
import requests

import requests
import pandas as pd
import os
from datetime import datetime
import zipfile
from zipfile import ZipFile, BadZipFile

import shutil


#------------------------------------------------------------------------------------------------------------------------------------
def is_point_within_aoi(lat, lon,aoi_polygon):
    point = Point(lon, lat)
    return point.within(aoi_polygon)
#------------------------------------------------------------------------------------------------------------------------------------

def download_sentinel3_L2_data(date_string: str, output_dir: str, ft: str) -> None:
    output_dir=os.path.join(output_dir,"S3_L2_OLCI")
    # Ensure the directory exists
    os.makedirs(output_dir, exist_ok=True)
    files_and_folders = os.listdir(output_dir)
    
    if files_and_folders:
        for item in files_and_folders:
            full_path = os.path.join(output_dir, item)
            os.remove(full_path) if os.path.isfile(full_path) else shutil.rmtree(full_path)
        print(f"Files and folders in {output_dir} deleted.")
    else:
        print(f"{output_dir} is empty.")

    def get_access(username: str, password: str) -> str:
        data = {
            "client_id": "cdse-public",
            "username": username,
            "password": password,
            "grant_type": "password",
        }
        try:
            r = requests.post("https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token",
                              data=data,
                              )
            r.raise_for_status()
        except Exception as e:
            raise Exception(
                f"Access token creation failed. Response from the server was: {r.json()}"
            )
        return r.json()["access_token"]

    #token = get_access("giorgiasalv@gmail.com", "S4rgi0tt0.82")  # INSERT CREDENTIALS
    token = get_access("chandu.nphdeo@gmail.com", "Indian@1234567890")

    formatted_date = datetime.strptime(date_string, "%Y%m%d").date()

    json = requests.get(f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products?"
                        f"$filter=contains(Name,'OL_2_LFR') and OData.CSC.Intersects(area=geography'SRID=4326;{ft}') "
                        f"and ContentDate/Start ge {formatted_date}T00:00:00.000Z "
                        f"and ContentDate/End le {formatted_date}T23:59:59.999Z").json()

    df = pd.DataFrame.from_dict(json["value"])
    
    session = requests.Session()
    session.headers.update({'Authorization': f'Bearer {token}'})

    # DOWNLOAD OF ALL PRODUCTS
    for index, row in df.iterrows():
        product_id = row['Id']
        product_name = row['Name']
        if "NT" in product_name:
            #print(f"Skipping download for {product_name} because it contains 'NT'")
            continue

        print(f"Downloading {product_name}.zip")

        output_info_dir=output_dir.replace('sentinel_product', 'sentinel_product_info')
        os.makedirs(output_info_dir, exist_ok=True)
        info_text_output_path = os.path.join(output_info_dir, f"in_product_names_{date_string}.txt")

        # Write the product name to the file
        with open(info_text_output_path, 'a+') as file:
            # Move the cursor to the end of the file
            file.seek(0, os.SEEK_END)
            # Check if the file is empty
            pos = file.tell()
            if pos > 0:
                # If not empty, move the cursor back to the end of the last line
                file.seek(pos - 1, os.SEEK_SET)
                # Ensure the last character is a newline
                last_char = file.read(1)
                if last_char != '\n':
                    file.write('\n')
            # Append the product name to the file
            file.write(product_name)
            # Add a newline character at the end
            file.write('\n')

        # Download the product using its ID
        url = f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products({product_id})/$value"
        response = session.get(url, allow_redirects=False)

        while response.status_code in (301, 302, 303, 307):
            url = response.headers['Location']
            response = session.get(url, allow_redirects=False)

        file = session.get(url, verify=False, allow_redirects=True)

        # Specify the full path for the ZIP file using the product name
        output_path = os.path.join(output_dir, f'{product_name}.zip')

        with open(output_path, 'wb') as p:
            p.write(file.content)

        #print(f"Downloaded ZIP file saved at: {output_path}")
    print("-" * 50)
    print(f"Downloaded ZIP files to : \"{output_dir}\"")
    
    # Get the list of all files in the folder
    all_files = os.listdir(output_dir)
    
    # Filter the ZIP files with "NR" tag and skip ".ipynb_checkpoints" files
    filtered_zip_files = [file for file in all_files if file.endswith(".SEN3.zip") and "NR" in file and ".ipynb_checkpoints" not in file]
    
    # Print the filtered ZIP files
    #print("Filtered ZIP files with 'NR' tag (skipping .ipynb_checkpoints):")
    #for file in filtered_zip_files:
    #    print(file)
    
    # Delete the non-matching files and empty directories
    for file in all_files:
        file_path = os.path.join(output_dir, file)
        if file not in filtered_zip_files:
            if os.path.isfile(file_path):
                os.remove(file_path)
                print(f"Deleted: {file_path}")
            elif os.path.isdir(file_path) and not os.listdir(file_path):
                os.rmdir(file_path)
                print(f"Deleted empty directory: {file_path}")

    # Get a list of all ZIP files in the folder
    zip_files = [file for file in os.listdir(output_dir) if file.endswith(".zip")]
    
    # Extract contents of each ZIP file to the same folder
    
    print(f"Extracting to \"{output_dir}\" ")
    
    for zip_file in zip_files:
        zip_file_path = os.path.join(output_dir, zip_file)
        try:
            with ZipFile(zip_file_path, 'r') as zip_ref:
                zip_ref.extractall(output_dir)
            #print(f"Extraction of {zip_file} complete.")
        except BadZipFile:
            print(f"Skipping {zip_file} as it is not a valid ZIP file.")

    unique_lines = set()
    # Read from the input file and store unique lines in the set
    with open(info_text_output_path, 'r') as input_file:
        for line in input_file:
            unique_lines.add(line.strip())
    
    # Write unique lines back to the original file
    with open(info_text_output_path, 'w') as output_file:
        for line in unique_lines:
            output_file.write(line + '\n')
    print("All L2 OLCI extractions complete.")
    print("-" * 50)



#------------------------------------------------------------------------------------------------------------------------------------

def download_sentinel3_L1_data(date_string: str, output_dir: str, aoi: str):
    output_dir=os.path.join(output_dir,"S3_L1_OLCI")
    # Ensure the directory exists
    os.makedirs(output_dir, exist_ok=True)
    files_and_folders = os.listdir(output_dir)
    if files_and_folders:
        for item in files_and_folders:
            full_path = os.path.join(output_dir, item)
            os.remove(full_path) if os.path.isfile(full_path) else shutil.rmtree(full_path)
        print(f"Files and folders in {output_dir} deleted.")
    else:
        print(f"{output_dir} is empty.")
    # Function to get access token
    def get_access(username: str, password: str) -> str:
        data = {
            "client_id": "cdse-public",
            "username": username,
            "password": password,
            "grant_type": "password",
        }
        try:
            r = requests.post("https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token",
                              data=data,
                              )
            r.raise_for_status()
        except Exception as e:
            raise Exception(
                f"Access token creation failed. Reponse from the server was: {r.json()}"
            )
        return r.json()["access_token"]

    spatial_filter_url = f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products?$filter=contains(Name,'OL_1_EFR____{date_string}') and OData.CSC.Intersects(area=geography%27SRID=4326;{aoi}%27)"

    # Get access token
    #token = get_access("giorgiasalv@gmail.com", "S4rgi0tt0.82")
    token = get_access("chandu.nphdeo@gmail.com", "Indian@1234567890")

    # Get product information from the spatial filter
    json = requests.get(spatial_filter_url).json()
    df = pd.DataFrame.from_dict(json["value"])

    # Create a session with the authorization header
    session = requests.Session()
    session.headers.update({'Authorization': f'Bearer {token}'})

    # Download each product and save it in the specified folder
    for index, row in df.iterrows():
        product_id = row['Id']
        product_name = row['Name']
        #print(product_name)
        if "NT" in product_name:
            #print(f"Skipping download for {product_name} because it contains 'NT'")
            continue

        print(f"Downloading {product_name}.zip")

        output_info_dir=output_dir.replace('sentinel_product', 'sentinel_product_info')
        os.makedirs(output_info_dir, exist_ok=True)
        info_text_output_path = os.path.join(output_info_dir, f"in_product_names_{date_string}.txt")

        # Write the product name to the file
        # Write the product name to the file
        with open(info_text_output_path, 'a+') as file:
            # Move the cursor to the end of the file
            file.seek(0, os.SEEK_END)
            # Check if the file is empty
            pos = file.tell()
            if pos > 0:
                # If not empty, move the cursor back to the end of the last line
                file.seek(pos - 1, os.SEEK_SET)
                # Ensure the last character is a newline
                last_char = file.read(1)
                if last_char != '\n':
                    file.write('\n')
            # Append the product name to the file
            file.write(product_name)
            # Add a newline character at the end
            file.write('\n')
            
        # Download the product using its ID
        url = f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products({product_id})/$value"
        response = session.get(url, allow_redirects=False)

        while response.status_code in (301, 302, 303, 307):
            url = response.headers['Location']
            response = session.get(url, allow_redirects=False)

        file = session.get(url, verify=False, allow_redirects=True)


        # Specify the full path for the ZIP file using the product name
        output_path = os.path.join(output_dir, f'{product_name}.zip')

        with open(output_path, 'wb') as p:
            p.write(file.content)

        #print(f"Downloaded ZIP file saved at: {output_path}")
    print("-" * 50)
    print(f"Downloaded ZIP files to : \"{output_dir}\"")
    
    # Get the list of all files in the folder
    all_files = os.listdir(output_dir)
    
    # Filter the ZIP files with "NR" tag and skip ".ipynb_checkpoints" files
    filtered_zip_files = [file for file in all_files if file.endswith(".SEN3.zip") and "NR" in file and ".ipynb_checkpoints" not in file]
    
    # Print the filtered ZIP files
    #print("Filtered ZIP files with 'NR' tag (skipping .ipynb_checkpoints):")
    #for file in filtered_zip_files:
    #    print(file)
    
    # Delete the non-matching files and empty directories
    for file in all_files:
        file_path = os.path.join(output_dir, file)
        if file not in filtered_zip_files:
            if os.path.isfile(file_path):
                os.remove(file_path)
                print(f"Deleted: {file_path}")
            elif os.path.isdir(file_path) and not os.listdir(file_path):
                os.rmdir(file_path)
                print(f"Deleted empty directory: {file_path}")

    # Get a list of all ZIP files in the folder
    zip_files = [file for file in os.listdir(output_dir) if file.endswith(".zip")]
    
    print(f"Extracting to \"{output_dir}\" ")
    
    # Extract contents of each ZIP file to the same folder
    for zip_file in zip_files:
        zip_file_path = os.path.join(output_dir, zip_file)
        try:
            with ZipFile(zip_file_path, 'r') as zip_ref:
                zip_ref.extractall(output_dir)
            #print(f"Extraction of {zip_file} complete.")
        except BadZipFile:
            print(f"Skipping {zip_file} as it is not a valid ZIP file.")

    unique_lines = set()
    # Read from the input file and store unique lines in the set
    with open(info_text_output_path, 'r') as input_file:
        for line in input_file:
            unique_lines.add(line.strip())
    
    # Write unique lines back to the original file
    with open(info_text_output_path, 'w') as output_file:
        for line in unique_lines:
            output_file.write(line + '\n')
            
    print("All L1 OLCI extractions complete.")
    print("-" * 50)


#------------------------------------------------------------------------------------------------------------------------------------
def download_sentinel3_SLSTR_L2_data(date_string: str, output_dir: str, ft: str) -> None:

    S3_L2_OLCI_dir=os.path.join(output_dir,"S3_L2_OLCI")
    S3_L2_OLCI_files = os.listdir(S3_L2_OLCI_dir)
    S3_L2_OLCI_sen3_files = [file for file in S3_L2_OLCI_files if file.endswith('.SEN3')]
    pattern = r'T(\d{6})'
    S3_OLCI_time_bounds = [re.search(pattern, file_name).group(1) for file_name in S3_L2_OLCI_sen3_files]
    
    # Ensure the directory exists
    output_dir=os.path.join(output_dir,"S3_L2_SLSTR")
    os.makedirs(output_dir, exist_ok=True)
    files_and_folders = os.listdir(output_dir)
    
    if files_and_folders:
        for item in files_and_folders:
            full_path = os.path.join(output_dir, item)
            os.remove(full_path) if os.path.isfile(full_path) else shutil.rmtree(full_path)
        print(f"Files and folders in {output_dir} deleted.")
    else:
        print(f"{output_dir} is empty.")

    def get_access(username: str, password: str) -> str:
        data = {
            "client_id": "cdse-public",
            "username": username,
            "password": password,
            "grant_type": "password",
        }
        try:
            r = requests.post("https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token",
                              data=data,
                              )
            r.raise_for_status()
        except Exception as e:
            raise Exception(
                f"Access token creation failed. Response from the server was: {r.json()}"
            )
        return r.json()["access_token"]

    #token = get_access("giorgiasalv@gmail.com", "S4rgi0tt0.82")  # INSERT CREDENTIALS
    token = get_access("chandu.nphdeo@gmail.com", "Indian@1234567890")

    formatted_date = datetime.strptime(date_string, "%Y%m%d").date()

    json = requests.get(f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products?"
                        f"$filter=contains(Name,'SL_2_LST') and OData.CSC.Intersects(area=geography'SRID=4326;{ft}') "
                        f"and ContentDate/Start ge {formatted_date}T00:00:00.000Z "
                        f"and ContentDate/End le {formatted_date}T23:59:59.999Z").json()

    df = pd.DataFrame.from_dict(json["value"])
    
    session = requests.Session()
    session.headers.update({'Authorization': f'Bearer {token}'})

    # DOWNLOAD OF ALL PRODUCTS
    for index, row in df.iterrows():
        product_id = row['Id']
        product_name = row['Name']
        if "NT" in product_name:
            #print(f"Skipping download for {product_name} because it contains 'NT'")
            continue

        if not any(olci_time in product_name for olci_time in S3_OLCI_time_bounds):
            #print("out of the temporal limit data")
            continue

        print(f"Downloading {product_name}.zip")

        output_info_dir=output_dir.replace('sentinel_product', 'sentinel_product_info')
        os.makedirs(output_info_dir, exist_ok=True)
        info_text_output_path = os.path.join(output_info_dir, f"in_product_names_{date_string}.txt")

        # Write the product name to the file
        with open(info_text_output_path, 'a+') as file:
            # Move the cursor to the end of the file
            file.seek(0, os.SEEK_END)
            # Check if the file is empty
            pos = file.tell()
            if pos > 0:
                # If not empty, move the cursor back to the end of the last line
                file.seek(pos - 1, os.SEEK_SET)
                # Ensure the last character is a newline
                last_char = file.read(1)
                if last_char != '\n':
                    file.write('\n')
            # Append the product name to the file
            file.write(product_name)
            # Add a newline character at the end
            file.write('\n')

        # Download the product using its ID
        url = f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products({product_id})/$value"
        response = session.get(url, allow_redirects=False)

        while response.status_code in (301, 302, 303, 307):
            url = response.headers['Location']
            response = session.get(url, allow_redirects=False)

        file = session.get(url, verify=False, allow_redirects=True)

        # Specify the full path for the ZIP file using the product name
        output_path = os.path.join(output_dir, f'{product_name}.zip')

        with open(output_path, 'wb') as p:
            p.write(file.content)

        #print(f"Downloaded ZIP file saved at: {output_path}") 

    print("-" * 50)
    print(f"Downloaded ZIP files to : \"{output_dir}\"")
    
    # Get the list of all files in the folder
    all_files = os.listdir(output_dir)
    
    # Filter the ZIP files with "NR" tag and skip ".ipynb_checkpoints" files
    filtered_zip_files = [file for file in all_files if file.endswith(".SEN3.zip") and "NR" in file and ".ipynb_checkpoints" not in file]
    
    # Print the filtered ZIP files
    #print("Filtered ZIP files with 'NR' tag (skipping .ipynb_checkpoints):")
    #for file in filtered_zip_files:
    #    print(file)
    
    # Delete the non-matching files and empty directories
    for file in all_files:
        file_path = os.path.join(output_dir, file)
        if file not in filtered_zip_files:
            if os.path.isfile(file_path):
                os.remove(file_path)
                #print(f"Deleted: {file_path}")
            elif os.path.isdir(file_path) and not os.listdir(file_path):
                os.rmdir(file_path)
                #print(f"Deleted empty directory: {file_path}")

    # Get a list of all ZIP files in the folder
    zip_files = [file for file in os.listdir(output_dir) if file.endswith(".zip")]
    
    print(f"Extracting to \"{output_dir}\" ")
    
    # Extract contents of each ZIP file to the same folder
    for zip_file in zip_files:
        zip_file_path = os.path.join(output_dir, zip_file)
        try:
            with ZipFile(zip_file_path, 'r') as zip_ref:
                zip_ref.extractall(output_dir)
            #print(f"Extraction of {zip_file} complete.")
            
        except BadZipFile:
            print(f"Skipping {zip_file} as it is not a valid ZIP file.")
    
    unique_lines = set()
    # Read from the input file and store unique lines in the set
    with open(info_text_output_path, 'r') as input_file:
        for line in input_file:
            unique_lines.add(line.strip())
    
    # Write unique lines back to the original file
    with open(info_text_output_path, 'w') as output_file:
        for line in unique_lines:
            output_file.write(line + '\n')
    print("All L2 SLSTR LST extractions complete.")
    print("-" * 50)



#_____________________________________________________________________________________________________________________________
def s3_download_L1_L2_OLCI_SLSTR(day_list, root_path, aoi_):
    tqdm.pandas()
    
    if aoi_=="po_basin":
        #lat_min, lat_max, lon_min, lon_max = 42.0, 48.0, 6.0, 15.0 #needed to change
        shapefile_path=shapefile_path = os.path.join(root_path, "utilities/AOI_po_basin_shape/Bacino_fiume_Po.shp")
        gdf=gpd.read_file(shapefile_path)
        lat_min = gdf.geometry.bounds['miny'].min()-0.5
        lat_max = gdf.geometry.bounds['maxy'].max()+0.5
        lon_min = gdf.geometry.bounds['minx'].min()-0.5
        lon_max = gdf.geometry.bounds['maxx'].max()+0.5
        
    elif aoi_ =="north_italy":    
        #lat_min, lat_max, lon_min, lon_max = 42.0, 48.0, 6.0, 15.0 #needed to change
        shapefile_path = os.path.join(root_path, "utilities/AOI_nord_italia_shape/nord_italia.shp")
        gdf=gpd.read_file(shapefile_path)
        lat_min = gdf.geometry.bounds['miny'].min()-0.5
        lat_max = gdf.geometry.bounds['maxy'].max()+0.5
        lon_min = gdf.geometry.bounds['minx'].min()-0.5
        lon_max = gdf.geometry.bounds['maxx'].max()+0.5
    elif aoi_ =="north_south_italy":
        #lat_min, lat_max, lon_min, lon_max = 42.0, 48.0, 6.0, 15.0 #needed to change
        shapefile_path =os.path.join(root_path, "utilities/AOI_sud_italia_shape/nord_sud_sicilia.shp")
        gdf=gpd.read_file(shapefile_path)
        lat_min = gdf.geometry.bounds['miny'].min()-0.5
        lat_max = gdf.geometry.bounds['maxy'].max()+0.5
        lon_min = gdf.geometry.bounds['minx'].min()-0.5
        lon_max = gdf.geometry.bounds['maxx'].max()+0.5
    else :
        print("AOI is undefined ....")


    
    for date_str in day_list:

        # Extract year, month, and day from the date string
        year = date_str[:4]
        month = date_str[4:6]
        day = date_str[6:]

    
        aoi_polygon = Polygon([(lon_min, lat_max), (lon_max, lat_max), (lon_max, lat_min), (lon_min, lat_min), (lon_min, lat_max)])
        aoi = f"POLYGON(({lon_min} {lat_min}, {lon_max} {lat_min}, {lon_max} {lat_max}, {lon_min} {lat_max}, {lon_min} {lat_min}))"

        target_day_string = f"{year}{month}{day}" 
        Sentinel3_download_folder = root_path + "IRIDE_SNOWCOVER_DATAFUSION_S3/sentinel_product/"

        # Download Sentinel_3 OLCI_2_ER Data
        download_sentinel3_L2_data(target_day_string, Sentinel3_download_folder, aoi)
        # Download Sentinel_3 OLCI_1_FR Data
        download_sentinel3_L1_data(target_day_string, Sentinel3_download_folder, aoi)
        # Download Sentinel_3 SLSTR_1_LST Data
        download_sentinel3_SLSTR_L2_data(target_day_string, Sentinel3_download_folder, aoi)

#_____________________________________________________________________________________________________________________________
if __name__ == "__main__" :
    
    import warnings
    warnings.filterwarnings("ignore")

    #day_list =[ "20240215", "20240217", "20240218", "20240220", "20240221", "20240225"] 
    day_list =[ "20240306"]
    day_list = [
        "20240308","20240307","20240306","20240305","20240304","20240303","20240302","20240301","20240229","20240228", "20240227", "20240226",
        "20240225", "20240224", "20240223", "20240222","20240221",
        "20240220", "20240219", "20240218", "20240217", "20240216", "20240215", "20240214","20240213",
        "20240212", "20240211", "20240210", "20240209", "20240208", "20240207", "20240206","20240205",
        "20240204", "20240203", "20240202", "20240201", "20240131", "20240130", "20240129", 
        "20240127", "20240126", "20240125", "20240124"
    ]
    day_list = [
        "20240303","20240302","20240301","20240229","20240228", "20240227", "20240226",
        "20240225", "20240224", "20240223", "20240222","20240221",
        "20240220", "20240219", "20240218", "20240217", "20240216", "20240215", "20240214","20240213",
        "20240212", "20240211", "20240210", "20240209", "20240208", "20240207", "20240206","20240205",
        "20240204", "20240203", "20240202", "20240201", "20240131", "20240130", "20240129", 
        "20240127", "20240126", "20240125", "20240124"
    ]

    day_list=[
        "20240318","20240317","20240316","20240315","20240314","20240313","20240312","20240311",
        "20240310","20240309","20240308","20240307","20240306","20240305","20240304","20240303",
        "20240302","20240301","20240229","20240228", "20240227", "20240226", "20240225","20240224",
        "20240223", "20240222","20240221","20240220", "20240219", "20240218", "20240217","20240216",
        "20240215", "20240214","20240213","20240212", "20240211", "20240210", "20240209","20240208",
        "20240207", "20240206","20240205","20240204", "20240203", "20240202", "20240201","20240131",
        "20240130", "20240129","20240128","20240127", "20240126", "20240125", "20240124"
    ]

    day_list =[ "20240306"]

    #["20240125", "20240131", "20240202", "20240205", "20240209", "20240213", "20240214", "20240215", "20240217", "20240218", "20240220", "20240221", "20240225"]
    root_path =""
    #lat_min, lat_max, lon_min, lon_max = 42.0, 48.0, 6.0, 15.0
    aoi_list=["po_basin","north_italy","north_south_italy"]
    aoi_=aoi_list[1]
    s3_download_L1_L2_OLCI_SLSTR(day_list[:], root_path, aoi_)