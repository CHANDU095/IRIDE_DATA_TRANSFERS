


import os
import shutil
import datetime
import glob
from enum import Enum
from xml.dom import minidom
from xml.etree import ElementTree as ET
import geopandas as gpd
import rasterio
from skimage.transform import resize
import numpy as np
from rasterio.enums import Resampling
from rasterio.warp import reproject
from rasterio.mask import mask
from shapely.geometry import shape
import numpy.ma as ma
import matplotlib.pyplot as plt
import pygrib
from osgeo import gdal
import time
from datetime import date, timedelta
from tqdm import tqdm
import pandas as pd
import xarray as xr
from shapely.geometry import mapping, Polygon, Point
from rasterio import features
import gzip
from ftplib import FTP
import cdsapi
import requests
from zipfile import ZipFile, BadZipFile
from datetime import datetime








#from sc_meta_data_gen_p2 import ConfigKeys,metadata_generation,get_aoi_by_date
from sc_meta_data_gen_p3 import ConfigKeys,metadata_generation,get_aoi_by_date


from s3_data_download import s3_download_L1_L2_OLCI_SLSTR
from s3_data_processing import s3_process_L1_L2_OLCI_SLSTR



from era_5_data_download import era5_data_download_parallel
from h35_data_download import h35_data_download_parallel

from sc_input_product_info_text_gen import concatenate_files_total_input_info_file

from era_5_data_processing import process_era_5_snowcover_data
from h35_data_processing import process_h35_snow_cover_data

from sc_datafusion import fusion_process_snowcover_data

from post_processing_nan_value import process_nan_values



XML_CONFIGS = {
    "svc" : "S5-02",
    "product_id" : "S5-02-05",
    "xml_folder" : "/media/fabspace/Volume/IRIDE/IrideMetadata/S5-02-05",
    "xml_parent" : "/media/fabspace/Volume/IRIDE/IrideMetadata",
    "service_name" : "snow_cover",
    "anchor_1_text" : "Meteorological geographical features",
    "title_text" : {
        "po_basin" : "Snow Cover binary mask, Po' Basin",
        "north_italy" : "Snow Cover binary mask, North Italy",
        "north_south_italy" : "Snow Cover binary mask, North and South of Italy",
        "italy" : "Snow Cover binary mask, Italy"
    },
    "abstract_string" : {
        "po_basin" : "Delivery: 1 of 4 - Partial AOI: Po' basin - Precursor AOI: Italy",
        "north_italy" : "Delivery: 2 of 4 - Partial AOI: North of Italy - Precursor AOI: Italy",
        "north_south_italy" : "Delivery: 3 of 4 - Partial AOI: North and South of Italy - Precursor AOI: Italy",
        "italy" : "Delivery: 4 of 4 - Italy"
    },
    "service_frequency" : "daily",
    "maintainance_delay" : "Time needed for availability of input data: min <= 1 day, max 6 days, depending on the availability of Sentinel-3, HSAF and ERA5",
    "resolution" : "3000-10000",
    "uom" : "m",
    "positional_accuracy" : "80",
    "thematic_accuracy" : "F1 score >= 80%",
    "statement_text" : {
        "po_basin" : "Mapping of Snow Cover based on EO data over the Po Basin Region",
        "north_italy" : "Mapping of Snow Cover based on EO data over the North of Italy",
        "north_south_italy" : "Mapping of Snow Cover based on EO data over the North and South of Italy",
        "italy" : "Mapping of Snow Cover based on EO data over Italy"
    },
    "product_url" : "https://landservices-dev.iride.earth/catalogue/#/dataset/2",
    "input_files_list" : [],
    "shapefile_path" : {
        "po_basin" : "/app/common/shapefiles/Bacino_fiume_Po/Bacino_fiume_Po.shp",
        "north_italy" : "/app/common/shapefiles/Nord_Italia_AOD/prova1.shp",
        "north_south_italy" : "/app/common/shapefiles/Nord_Sud_Italia/nord_sud_sicilia.shp",
        "italy" : "/app/common/shapefiles/Nord_Sud_Italia/nord_sud_sicilia.shp"
    },
    "reference_time" : "",
    "aoi" : "po_basin",
    "hsaf_input_id" : "[IN-S5-02-15] HSAF",
    "era5_input_id" : "[IN-S5-02-20] ERA5/SNOWCOVER",
    "s3_input_id_l1" : "[IN-S5-02-02] Sentinel-3/L1 OLCI",
    "s3_input_id_l2" : "[IN-S5-02-02] Sentinel-3/L2 OLCI",
    "s3_l1_resolution" : "300",
    "s3_l1_uom" : "m",
    "s3_l2_resolution" : "300",
    "s3_l2_uom" : "m",
    "hsaf_resolution" : "1113",
    "hsaf_uom" : "m",
    "era5_resolution" : "9000",
    "era5_uom" : "m"
}
    






def copy_tif_files(source_dir, destination_dir):
    if not os.path.exists(destination_dir):
        os.makedirs(destination_dir)

    for file_name in os.listdir(source_dir):
        if file_name.startswith("IRIDE") and file_name.endswith(".tif"):
            source_file = os.path.join(source_dir, file_name)
            destination_file = os.path.join(destination_dir, file_name)
            shutil.copyfile(source_file, destination_file)


if __name__ == "__main__" :
    import warnings
    warnings.filterwarnings("ignore")

    
    root_path =""
    #date_strs = [ "20240318","20240317"]
    """
    date_strs = [
        "20240430", "20240429", "20240428", "20240427", "20240426", "20240425", "20240424",
        "20240423", "20240422", "20240421", "20240420", "20240419", "20240418", "20240417",
        "20240416", "20240415", "20240414", "20240413", "20240412", "20240411", "20240410",
        "20240409", "20240408", "20240407", "20240406", "20240405", "20240404", "20240403",
        "20240402", "20240401", "20240331", "20240330", "20240329", "20240328", "20240327",
        "20240326", "20240325", "20240324", "20240323", "20240322", "20240321", "20240320",
        "20240319", "20240318", "20240317", "20240316", "20240315", "20240314", "20240313",
        "20240312", "20240311", "20240310", "20240309", "20240308", "20240307", "20240306",
        "20240305", "20240304", "20240303", "20240302", "20240301", "20240229", "20240228",
        "20240227", "20240226", "20240225", "20240224", "20240223", "20240222", "20240221",
        "20240220", "20240219", "20240218", "20240217", "20240216", "20240215", "20240214",
        "20240213", "20240212", "20240211", "20240210", "20240209", "20240208", "20240207",
        "20240206", "20240205", "20240204", "20240203", "20240202", "20240201", "20240131",
        "20240130", "20240129", "20240128", "20240127", "20240126", "20240125", "20240124"
        ]

    """
    date_strs =["20240329"]


    for date_str in date_strs[:]:
        year = int(date_str[:4])
        month = int(date_str[4:6])
        day = int(date_str[6:])

        #REFERENCE_TIME = "20240222"
        REFERENCE_TIME = f"{year}{month:02d}{day:02d}"
        
        aoi = get_aoi_by_date(year, day, month)
        print(aoi)
        
        #download_process_s3_data_to_tiffs([date_str], root_path,aoi)

        s3_download_L1_L2_OLCI_SLSTR([date_str], root_path,aoi)
        era5_data_download_parallel([date_str], root_path)
        h35_data_download_parallel([date_str], root_path)
        
        concatenate_files_total_input_info_file(root_path,date_str)

        s3_process_L1_L2_OLCI_SLSTR([date_str], root_path,aoi)
        process_era_5_snowcover_data(root_path,date_str,aoi)
        process_h35_snow_cover_data(date_str, root_path,aoi)
        
        fusion_process_snowcover_data(date_str, root_path,aoi)

        # Example usage:
        my_prod_version_dir="IRIDE_SNOWCOVER_DATAFUSION_PRODUCTS"#"IRIDE_SNOWCOVER_DATAFUSION_PRODUCTS"
        source_dir = os.path.join(root_path, f"{my_prod_version_dir}/{year}-{month:02d}-{day:02d}/")
        destination_dir = os.path.join(root_path, f"{my_prod_version_dir}/products_tiffs/")
        copy_tif_files(source_dir, destination_dir)

        product_tiff_path=os.path.join(destination_dir,f"IRIDE-S_S5-02-05_{date_str}_V0.tif")
        process_nan_values(product_tiff_path)

        
        XML_FOLDER = destination_dir
        XML_PARENT = destination_dir
        input_products_info_path =os.path.join(root_path, f"Input_products_info_text_files")
        INPUT_TEXT_PATH = os.path.join(input_products_info_path,f"S5-02-05_inputs_{REFERENCE_TIME}.txt")
        
        print("\nXML_FOLDER= "+ XML_FOLDER +"\nXML_PARENT = "+ XML_PARENT +"\nINPUT_TEXT_PATH = "+INPUT_TEXT_PATH)
        
        XML_CONFIGS["reference_time"] = REFERENCE_TIME
        XML_CONFIGS["xml_folder"] = XML_FOLDER
        XML_CONFIGS["xml_parent"] = XML_PARENT
        
        if aoi in [ConfigKeys.PO_BASIN.value, ConfigKeys.NORTH_ITALY.value, ConfigKeys.NORTH_SOUTH_ITALY.value, ConfigKeys.ITALY.value]:
            shapefile_path = XML_CONFIGS[ConfigKeys.SHAPEFILE_PATH.value][aoi]
            XML_CONFIGS[ConfigKeys.AOI.value] = aoi
        else:
            #shapefile_path = configs[ConfigKeys.SHAPEFILE_PATH.value]
            shapefile_path = XML_CONFIGS[ConfigKeys.SHAPEFILE_PATH.value]
        metadata_generation(
            xml_configs = XML_CONFIGS,
            input_text_path = INPUT_TEXT_PATH,
        )









        
        

        
        
        


        
        
        

