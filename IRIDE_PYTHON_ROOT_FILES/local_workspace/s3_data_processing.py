
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


#------------------------------------------------------------------------------------------------------------------------------------
def sen_3_SLSTR_LST_extraction(folder_path,aoi_polygon):
    
    # List all files in the directory
    sen3_files = os.listdir(folder_path)
    #print(sen3_files)
    # Filter only .sen3 files
    sen3_files = [file for file in sen3_files if file.endswith('.SEN3')]
    # Initialize an empty DataFrame to store the results
    final_df = pd.DataFrame()
    # Check if there are multiple files
    if len(sen3_files) >= 1:
        # Iterate through each .sen3 file
        count=1
        for sen3_file in sen3_files:
            # Construct the full path to the .sen3 file
            
            print("-" * 50)
            print(f"Product <{count}> Processing for LST")
            count+=1
            print("-" * 50)
            sen3_path = os.path.join(folder_path, sen3_file)
            product_files = os.listdir(sen3_path)
            # Filter only NetCDF files
            nc_files = [file for file in product_files if file.endswith('.nc')]
            #print(nc_files)
            # Initialize variables for latitude, longitude, and band_data
            latitude = None
            longitude = None
            lst = None
            # Iterate through each NetCDF file
            #print("-" * 50)
            for nc_file in nc_files:
                # Construct the full path to the NetCDF file
                nc_path = os.path.join(sen3_path, nc_file)
        
                with xr.open_dataset(nc_path) as ds:
                    if 'geodetic_in.nc' in nc_file :
                        if 'latitude_in' in ds.variables and 'longitude_in' in ds.variables:
                            latitude = ds['latitude_in'].values
                            longitude = ds['longitude_in'].values
                            #print(f"Latitudes {latitude.shape}, Longitudes {longitude.shape}")
                            #print("-" * 50)
                    if "LST_in.nc" in nc_file :
                        if 'LST' in ds.variables :
                            LST_array=ds['LST'].values
                            #print(f"LST Data shape : {LST_array.shape}")
                            #print("-" * 50)
                        
       
            DF = pd.DataFrame({
                'lst_lon': longitude.flatten(),
                'lst_lat': latitude.flatten(),
                'lst': LST_array.flatten()
            })
            final_df = pd.concat([final_df, DF], ignore_index=True)

    #print("the S3_L2_SLSTR dataframe : \n",DF)
    del DF,latitude,longitude,LST_array

    print("Defining Geometry  ")
    final_gdf = gpd.GeoDataFrame(final_df, geometry=gpd.points_from_xy(final_df["lst_lon"], final_df["lst_lat"]))
    print("Subsetting with the AOI")
    if isinstance(aoi_polygon, gpd.GeoDataFrame):
        aoi_polygon = aoi_polygon.geometry.unary_union
    # Filter points within AOI
    start = time.time()
    final_gdf = final_gdf[final_gdf.within(aoi_polygon)]
    end = time.time()
    print(f"Subsetting_time taken is {end - start} seconds")
    #Dropping geomtery for now to recreat with all together
    final_df = final_gdf.drop(columns=['geometry'])
            

    return final_df


#------------------------------------------------------------------------------------------------------------------------------------

###

#------------------------------------------------------------------------------------------------------------------------------------

def sen_3_L1_ndsi_snow_cover_calculation(folder_path,aoi_polygon, ndsi_threshold=0.163,rBRR_02_threshold=0.4):
    # List all files in the directory
    sen3_files = os.listdir(folder_path)
    #print(sen3_files)
    # Filter only .sen3 files
    sen3_files = [file for file in sen3_files if file.endswith('.SEN3')]
    # Initialize variables for latitude, longitude, and band_data
    latitude = None
    longitude = None
    band_02_data = None
    band_04_data = None
    band_06_data = None
    band_08_data = None
    band_17_data = None
    band_21_data = None
    quality_flags_land = None

    # Initialize an empty DataFrame to store the results
    final_df = pd.DataFrame()
    count=1
    # Check if there are multiple files
    if len(sen3_files) >= 1:
        # Iterate through each .sen3 file
        for sen3_file in sen3_files:
            # Construct the full path to the .sen3 file
            sen3_path = os.path.join(folder_path, sen3_file)
            product_files = os.listdir(sen3_path)
            # Filter only NetCDF files
            nc_files = [file for file in product_files if file.endswith('.nc')]

            # Reset variables for each file
            latitude = None
            longitude = None
            band_02_data = None
            band_04_data = None
            band_06_data = None
            band_08_data = None
            band_17_data = None
            band_21_data = None
            quality_flags_land = None

            for nc_file in nc_files:
                nc_path = os.path.join(sen3_path, nc_file)
                with xr.open_dataset(nc_path) as ds:
                    # Check if latitude and longitude are present
                    if 'geo_coordinates.nc' in nc_file and 'tie_geo_coordinates.nc' not in nc_file:
                        if 'latitude' in ds.variables and 'longitude' in ds.variables:
                            latitude = ds['latitude'].values
                            longitude = ds['longitude'].values
                            #print(f"Latitudes {latitude.shape}, Longitudes {longitude.shape}")
                            #print("-" * 50)
        
                    elif 'Oa02_radiance.nc' in nc_file and 'Oa02_radiance' in ds.variables:
                        variable_name = 'Oa02_radiance'
                        band_02_data = ds[variable_name].values
                        #print(f"Band 2 is loaded as {band_02_data.shape}")
                        #print("-" * 50)
                    elif 'Oa04_radiance.nc' in nc_file and 'Oa04_radiance' in ds.variables:
                        variable_name = 'Oa04_radiance'
                        band_04_data = ds[variable_name].values
                        #print(f"Band 4(B) is loaded as {band_04_data.shape}")
                        #print("-" * 50)
                    elif 'Oa06_radiance.nc' in nc_file and 'Oa06_radiance' in ds.variables:
                        variable_name = 'Oa06_radiance'
                        band_06_data = ds[variable_name].values
                        #print(f"Band 6(G) is loaded as {band_06_data.shape}")
                        #print("-" * 50)
                    elif 'Oa08_radiance.nc' in nc_file and 'Oa08_radiance' in ds.variables:
                        variable_name = 'Oa08_radiance'
                        band_08_data = ds[variable_name].values
                        #print(f"Band 8(R) is loaded as {band_08_data.shape}")
                        #print("-" * 50)
                    elif 'Oa17_radiance.nc' in nc_file and 'Oa17_radiance' in ds.variables:
                        variable_name = 'Oa17_radiance'
                        band_17_data = ds[variable_name].values
                        #print(f"Band 17 is loaded as {band_17_data.shape}")
                        #print("-" * 50)
                    elif 'Oa21_radiance.nc' in nc_file and 'Oa21_radiance' in ds.variables:
                        variable_name = 'Oa21_radiance'
                        band_21_data = ds[variable_name].values
                        #print(f"Band 21 is loaded as {band_21_data.shape}")
                        #print("-" * 50)
                    elif 'qualityFlags.nc' in nc_file and 'quality_flags' in ds.variables:
                        variable_name = 'quality_flags'
                        quality_flags = ds[variable_name].values
                        #print(f"quality_flags is loaded as {quality_flags.shape}")
                        #print("-" * 50)

            print("-" * 50)
            print(f"Product <{count}> Processing quality_flags to obtain Land/Sea Mask")
            quality_flags_land_binary_masks = 2147483648  # Land/Sea flag
            # Calculate mask
            quality_flags_land = (quality_flags & quality_flags_land_binary_masks) != 0
            print("Land_Sea Mask Obtained....")
            print("Calculating NDSI and the SnowCover")
            rBRR_02 = band_02_data
            rBRR_17 = band_17_data
            rBRR_21 = band_21_data
            rBRR_02 = np.nan_to_num(rBRR_02, nan=0)
            rBRR_17 = np.nan_to_num(rBRR_17, nan=0)
            rBRR_21 = np.nan_to_num(rBRR_21, nan=0)
            NDSI = np.divide((rBRR_17 - rBRR_21), (rBRR_17 + rBRR_21), out=np.zeros_like(rBRR_17), where=(rBRR_17 + rBRR_21) != 0)
            #print("NDSI Obtained ")
            
            # Perform the specified condition on NDSI
            condition = (NDSI > ndsi_threshold) & (rBRR_02 > rBRR_02_threshold) & quality_flags_land
            print(f"Snow_Cover Obtained thresholded @ NDSI > {ndsi_threshold} and rBRR_02 > {rBRR_02_threshold}")
            #print("-" * 50)
            # Create a boolean array with True for elements that satisfy the condition and False otherwise
            snow_cover_array = np.where(condition, True, False)
            # Print or use the result_array as needed
            print("Snow Cover Array Shape : ", snow_cover_array.shape)
            print("-" * 50)
            # Create a DataFrame
            DF = pd.DataFrame({
                'lon': longitude.flatten(),
                'lat': latitude.flatten(),
                'snow_cover': snow_cover_array.flatten()
            })   
            final_df = pd.concat([final_df, DF], ignore_index=True)
        #print("the L1 dataframe : \n",DF.head())
        del DF,latitude,longitude,snow_cover_array,condition,NDSI,rBRR_21,rBRR_17,rBRR_02,band_21_data,band_17_data,band_02_data,quality_flags_land_binary_masks
        print("Defining Geometry  ")
        final_gdf = gpd.GeoDataFrame(final_df, geometry=gpd.points_from_xy(final_df["lon"], final_df["lat"]))
        print("Subsetting with the AOI")
        if isinstance(aoi_polygon, gpd.GeoDataFrame):
            aoi_polygon = aoi_polygon.geometry.unary_union
        # Filter points within AOI
        start = time.time()
        final_gdf = final_gdf[final_gdf.within(aoi_polygon)]
        end = time.time()
        print(f"Subsetting_time taken is {end - start} seconds")
        print("-" * 50)
        #Dropping geomtery for now to recreat with all together
        final_df = final_gdf.drop(columns=['geometry'])
        
        return final_df

#------------------------------------------------------------------------------------------------------------------------------------

def sen_3_L2_SC_CC_mask_extraction(folder_path,aoi_polygon):

    # List all files in the directory
    sen3_files = os.listdir(folder_path)
    #print(sen3_files)
    # Filter only .sen3 files
    sen3_files = [file for file in sen3_files if file.endswith('.SEN3')]
    # Initialize an empty DataFrame to store the results
    final_df = pd.DataFrame()
    # Check if there are multiple files
    if len(sen3_files) >= 1:
        # Iterate through each .sen3 file
        for sen3_file in sen3_files:
            # Construct the full path to the .sen3 file
            sen3_path = os.path.join(folder_path, sen3_file)
            product_files = os.listdir(sen3_path)
            # Filter only NetCDF files
            nc_files = [file for file in product_files if file.endswith('.nc')]
            #print(nc_files)
            # Initialize variables for latitude, longitude, and band_data
            latitude = None
            longitude = None
            quality_flags_land = None
            # Iterate through each NetCDF file
            print("-" * 50)
            for nc_file in nc_files:
                # Construct the full path to the NetCDF file
                nc_path = os.path.join(sen3_path, nc_file)
        
                with xr.open_dataset(nc_path) as ds:
                    if 'geo_coordinates.nc' in nc_file and 'tie_geo_coordinates.nc' not in nc_file:
                        if 'latitude' in ds.variables and 'longitude' in ds.variables:
                            latitude = ds['latitude'].values
                            longitude = ds['longitude'].values
                            #print(f"Latitudes {latitude.shape}, Longitudes {longitude.shape}")
                            #print("-" * 50)
                            
                    elif 'lqsf.nc' in nc_file and 'LQSF' in ds.variables:
                        variable_name = 'LQSF'
                        quality_flags = ds[variable_name].values
                        #print(f"quality_flags is loaded as {quality_flags.shape}")
                        #print("-" * 50)
                        
            print("Processing quality_flags to obtain Cloud and Snow/Ice Masks")
            # Define binary masks
            cloud_mask_binary_mask = 8  # Cloud flag
            snow_ice_mask_binary_mask = 16  # Snow/Ice flag
            # Calculate masks
            cloud_mask = (quality_flags & cloud_mask_binary_mask) != 0
            snow_ice_mask = (quality_flags & snow_ice_mask_binary_mask) != 0
            DF = pd.DataFrame({
                'L2_lon': longitude.flatten(),
                'L2_lat': latitude.flatten(),
                'L2_snow_cover': snow_ice_mask.flatten(),
                'L2_cloud_cover': cloud_mask.flatten()
            })
            final_df = pd.concat([final_df, DF], ignore_index=True)

    #print("the L2 dataframe : \n",DF)
    del DF,latitude,longitude,snow_ice_mask,cloud_mask


    print("Defining Geometry  ")
    final_gdf = gpd.GeoDataFrame(final_df, geometry=gpd.points_from_xy(final_df["L2_lon"], final_df["L2_lat"]))
    print("Subsetting with the AOI")
    if isinstance(aoi_polygon, gpd.GeoDataFrame):
        aoi_polygon = aoi_polygon.geometry.unary_union
    # Filter points within AOI
    start = time.time()
    final_gdf = final_gdf[final_gdf.within(aoi_polygon)]
    end = time.time()
    print(f"Subsetting_time taken is {end - start} seconds")
    #Dropping geomtery for now to recreat with all together
    final_df = final_gdf.drop(columns=['geometry'])
    return final_df




#__________________________________________________________________________________________________________ 

    

def sen_3_dataframe_merge_export_to_tiff(OLCI_L1_DF,OLCI_L2_DF,SLSTR_L2_DF,root_dir,output_dir,aoi_polygon,date_str,aoi):

    os.makedirs(output_dir, exist_ok=True)
    files_and_folders = os.listdir(output_dir)
    if files_and_folders:
        for item in files_and_folders:
            full_path = os.path.join(output_dir, item)
            os.remove(full_path) if os.path.isfile(full_path) else shutil.rmtree(full_path)
        print(f"Files and folders in {output_dir} deleted.")
    else:
        print(f"{output_dir} is empty.")
    OLCI_L1_DF = OLCI_L1_DF.sort_values(by=['lat', 'lon'])
    OLCI_L1_DF.reset_index(drop=True, inplace=True)
    OLCI_L2_DF = OLCI_L2_DF.sort_values(by=['L2_lat', 'L2_lon'])
    OLCI_L2_DF.reset_index(drop=True, inplace=True)
    len_df1 = len(OLCI_L1_DF)
    len_df2 = len(OLCI_L2_DF)
    if len_df1 > len_df2:
        OLCI_L1_DF = OLCI_L1_DF[:len_df2]
    elif len_df2 > len_df1:
        OLCI_L2_DF = OLCI_L2_DF[:len_df1]
    target_x_pixel_size = 0.011131580833333335
    merged_df = pd.merge(OLCI_L1_DF, OLCI_L2_DF, how='inner', left_index=True, right_index=True)
    #print("the L1L2 dataframe : \n",merged_df)
    merged_df = merged_df.drop(['L2_lon', 'L2_lat'], axis=1)
    #print("the L1L2 dataframe filtered : \n",merged_df)

    
    merged_df = merged_df.drop_duplicates(subset=['lat', 'lon'], keep='first')
    merged_df = merged_df.sort_values(by=['lat', 'lon'])
    

    
    
    SC=merged_df['snow_cover']
    #SC=merged_df['L2_snow_cover']
    L2_CC=merged_df['L2_cloud_cover']
    SC[L2_CC==True]=np.nan
    merged_df['snow_cover']=SC

    SLSTR_grid_points=SLSTR_L2_DF[["lst_lon","lst_lat"]].values
    SLSTR_grid_values=SLSTR_L2_DF["lst"].values
    OLCI_xi=merged_df[["lon","lat"]].values

    print("Resampling the SLSTR_LST data to OLCI")
    LST_interpolated=griddata(SLSTR_grid_points,SLSTR_grid_values,OLCI_xi,method="linear")
    merged_df["lst"]=LST_interpolated

    merged_df.loc[(merged_df['snow_cover'] == True) & (~np.isnan(merged_df['lst'])) & (merged_df['lst'] < 273.15), 'snow_cover'] = False

    

    

    #print("the L1L2 dataframe filtered cloud_covered: \n",merged_df)
    print("b1================================= ",len(merged_df))
    
    print("Subsetting with the AOI")
    
    start=time.time()
    subset_merged_gdf=gpd.GeoDataFrame(merged_df,geometry=gpd.points_from_xy(merged_df["lon"],merged_df["lat"]))
    
    if isinstance(aoi_polygon,gpd.GeoDataFrame):
        aoi_polygon=aoi_polygon.geometry.unary_union

    subset_merged_gdf=subset_merged_gdf[subset_merged_gdf.within(aoi_polygon)]
    subset_merged_df=subset_merged_gdf.drop(columns=["geometry"])
    #print("the L1L2 dataframe filtered cloud_covered subsetted: \n",subset_merged_df)
    longitude=subset_merged_df['lon']
    latitude=subset_merged_df['lat']
    end=time.time()
    print(f"Subsetting_time taken is {end-start} seconds")

    print("b2================================= ",len(subset_merged_df))
    
    del OLCI_L1_DF, OLCI_L2_DF, SLSTR_L2_DF, merged_df, SC, L2_CC, SLSTR_grid_points, SLSTR_grid_values, OLCI_xi, subset_merged_gdf
    
    # Calculate pixel sizes
    xmin, ymin, xmax, ymax = longitude.min(), latitude.min(), longitude.max(), latitude.max()
    x_pixel_size = target_x_pixel_size
    y_pixel_size = target_x_pixel_size
    ncol = int((longitude.max() - longitude.min()) / x_pixel_size)
    nrow = int((latitude.max() - latitude.min()) / y_pixel_size)
    
    #subset_merged_df = subset_merged_df.drop_duplicates(subset=['lat', 'lon'], keep='first')
    #subset_merged_df = subset_merged_df.sort_values(by=['lat', 'lon'])
    
    print(f"Geometery Defining is processing.....")
    start=time.time()
    # Convert Shapely geometry to GeoJSON-like format
    subset_merged_df['geometry'] = [mapping(Polygon(
        [(lon, lat), (lon + x_pixel_size, lat), (lon + x_pixel_size, lat - y_pixel_size), (lon, lat - y_pixel_size)]))
                          for lon, lat in tqdm(zip(subset_merged_df['lon'], subset_merged_df['lat']))]
    
    end=time.time()
    print(f"Geometery Definition_time taken is {end-start} seconds")
    del longitude, latitude
    print("b3================================= ",len(subset_merged_df))
    

    
    # Set up a raster using rasterio with correct pixel sizes
    output_raster = os.path.join(output_dir, f"sentinel_{date_str}.tif")
    with rasterio.open(
            output_raster,
            'w',
            driver='GTiff',
            height=nrow,
            width=ncol,
            count=1,
            dtype=rasterio.float64,
            crs='EPSG:4326',
            transform=rasterio.transform.from_origin(xmin, ymax, x_pixel_size, y_pixel_size),
            nodata=np.nan
    ) as dst:
        shapes = ((geom, val) for geom, val in zip(subset_merged_df['geometry'], subset_merged_df['snow_cover']))
        burned = features.rasterize(
            shapes=shapes,
            out_shape=(nrow, ncol),
            transform=rasterio.transform.from_origin(xmin, ymax, x_pixel_size, y_pixel_size),
            all_touched=True
        )
        dst.write(burned, 1)

    print(f"Processed: {output_raster}")

    if aoi=="po_basin":
        shapefile_path=shapefile_path = os.path.join(root_dir, "utilities/AOI_po_basin_shape/Bacino_fiume_Po.shp")
    elif aoi =="north_italy":    
        shapefile_path = os.path.join(root_dir, "utilities/AOI_nord_italia_shape/nord_italia.shp")
    elif aoi =="north_south_italy":
        shapefile_path =os.path.join(root_dir, "utilities/AOI_sud_italia_shape/nord_sud_sicilia.shp")
    else :
        print("AOI is undefined ....")

    with rasterio.open(output_raster) as src:
        shapefile = gpd.read_file(shapefile_path)
        #transform the crs of the nord_italia shape file only has a diverfy from the rest
        if aoi=="north_italy":
            shapefile = shapefile.to_crs(epsg=4326)
        
        
        clipped_image, clipped_transform = mask(src, shapefile.geometry, crop=True)
        clipped_profile = src.profile
        clipped_profile.update({
            "height": clipped_image.shape[1],
            "width": clipped_image.shape[2],
            "transform": clipped_transform
        })
        clipped_filename = os.path.join(output_dir, f"Clipped_sentinel_{date_str}.tif")
        # Save clipped image to a new TIFF file
        with rasterio.open(clipped_filename, "w", **clipped_profile) as dst:
            dst.write(clipped_image)

         #____******_____EXTRA CHECKING PLOTS 

        # Create a Basemap object with the specified bounds 
        m = Basemap(llcrnrlon=xmin, llcrnrlat=ymin,
                    urcrnrlon=xmax, urcrnrlat=ymax,
                    resolution='l', projection='merc')
        x, y = m(subset_merged_df['lon'].values, subset_merged_df['lat'].values)
        # Plot the data as points on the map
        plt.figure(figsize=(10, 8))
        m.scatter(x, y, c=subset_merged_df['lst'], cmap='viridis', s=10, alpha=0.5)
        plt.colorbar(label='Temperature (K)')
        m.drawcoastlines()
        m.drawcountries()
        plt.title(f'LST_DISTRIBUTION_{date_str}')
        plt.savefig(os.path.join(output_dir, f"LST_DISTRIBUTION_{date_str}.jpg"))
        #plt.show()
    
        # Plot the data as points on the map
        plt.figure(figsize=(10, 8))
        m.scatter(x, y, c=subset_merged_df['snow_cover'], cmap='viridis', s=10, alpha=0.5)
        #plt.colorbar(label='Temperature (K)')
        m.drawcoastlines()
        m.drawcountries()
        plt.title(f'NDSI_SNOWCOVER_{date_str}')
        plt.savefig(os.path.join(output_dir, f"NDSI_SNOWCOVER_{date_str}.jpg"))
        #plt.show()
    
        # Plot the data as points on the map
        plt.figure(figsize=(10, 8))
        m.scatter(x, y, c=subset_merged_df['L2_snow_cover'], cmap='viridis', s=10, alpha=0.5)
        #plt.colorbar(label='Temperature (K)')
        m.drawcoastlines()
        m.drawcountries()
        plt.title(f'L2_SNOWCOVER_{date_str}')
        plt.savefig(os.path.join(output_dir, f"L2_SNOWCOVER_{date_str}.jpg"))
        #plt.show()
    
        # Plot the data as points on the map
        plt.figure(figsize=(10, 8))
        m.scatter(x, y, c=subset_merged_df['L2_cloud_cover'], cmap='viridis', s=10, alpha=0.5)
        #plt.colorbar(label='Temperature (K)')
        m.drawcoastlines()
        m.drawcountries()
        plt.title(f'L2_CLOUD_COVER_{date_str}')
        plt.savefig(os.path.join(output_dir, f"L2_CLOUDCOVER_{date_str}.jpg"))
        #plt.show()


        
        

    




#_____________________________________________________________________________________________________________________________
def s3_process_L1_L2_OLCI_SLSTR(day_list, root_path, aoi_):
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
        #download_sentinel3_L2_data(target_day_string, Sentinel3_download_folder, aoi)
        # Download Sentinel_3 OLCI_1_FR Data
        #download_sentinel3_L1_data(target_day_string, Sentinel3_download_folder, aoi)
        # Download Sentinel_3 SLSTR_1_LST Data
        #download_sentinel3_SLSTR_L2_data(target_day_string, Sentinel3_download_folder, aoi)

        S3_output_tiffs = root_path + f"IRIDE_SNOWCOVER_DATAFUSION_S3/Final_SEN3_Tiffs/{year}{month}{day}"

        OLCI_L1_products_directory = Sentinel3_download_folder + 'S3_L1_OLCI/'
        OLCI_L2_products_directory = Sentinel3_download_folder + 'S3_L2_OLCI/'
        SLSTR_L2_products_directory = Sentinel3_download_folder + 'S3_L2_SLSTR/'

        OLCI_L1_DF = sen_3_L1_ndsi_snow_cover_calculation(OLCI_L1_products_directory, ndsi_threshold=0.163, aoi_polygon=aoi_polygon)
        OLCI_L2_DF = sen_3_L2_SC_CC_mask_extraction(OLCI_L2_products_directory, aoi_polygon)
        SLSTR_L2_DF=sen_3_SLSTR_LST_extraction(SLSTR_L2_products_directory,aoi_polygon)

        #print("the OLCI_L1 dataframe : \n", OLCI_L1_DF)
        #print("the OLCI_L2 dataframe : \n", OLCI_L2_DF)
        #print("the SLSTR_L2 dataframe : \n", SLSTR_L2_DF)

        #date_str = f"{year}{month}{day}"
        sen_3_dataframe_merge_export_to_tiff(OLCI_L1_DF, OLCI_L2_DF,SLSTR_L2_DF, root_path, S3_output_tiffs, aoi_polygon=aoi_polygon, date_str=date_str, aoi=aoi_)

        del OLCI_L1_DF
        del OLCI_L2_DF
        del SLSTR_L2_DF

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
    s3_process_L1_L2_OLCI_SLSTR(day_list[:], root_path, aoi_)