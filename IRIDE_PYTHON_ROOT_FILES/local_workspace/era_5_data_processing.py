import os
import argparse
import xarray as xr
import numpy as np
import rasterio
from rasterio.transform import from_origin
import geopandas as gpd
from rasterio.mask import mask
from osgeo import gdal
import matplotlib.pyplot as plt
from rasterio.plot import show

def process_era_5_snowcover_data(root_dir,date_str,aoi):

    # Extract year, month, and day from the date string
    year = date_str[:4]
    month = date_str[4:6]
    day = date_str[6:]

    date_str=f"{year}-{month}-{day}"
    product_dir=os.path.join(root_dir,f"IRIDE_SNOWCOVER_DATAFUSION_ERA5/era5_products/{date_str}")
    output_dir= os.path.join(root_dir,f"IRIDE_SNOWCOVER_DATAFUSION_ERA5/Final_era5_Tifs/{date_str}")
    
    
    # Product Path
    product_path = os.path.join(product_dir, f"era5_{date_str}.nc")
    
    if aoi=="po_basin":
        shapefile_path=shapefile_path = os.path.join(root_dir, "utilities/AOI_po_basin_shape/Bacino_fiume_Po.shp")
    elif aoi =="north_italy":    
        shapefile_path = os.path.join(root_dir, "utilities/AOI_nord_italia_shape/nord_italia.shp")
    elif aoi =="north_south_italy":
        shapefile_path =os.path.join(root_dir, "utilities/AOI_sud_italia_shape/nord_sud_sicilia.shp")
    else :
        print("AOI is undefined ....")
    
    # Open dataset
    ds = xr.open_dataset(product_path)
    
    # Data extraction
    snowcover = ds["snowc"].values[1, :, 0:1800]
    lons = ds["longitude"].values[0:1800]
    lats = ds["latitude"].values
    
    # Convert snowcover array to binary
    threshold = 70.0
    snowcover_binary = np.where(np.isnan(snowcover), np.nan, (snowcover > threshold).astype(int))
    
    # Output directory creation
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Define the GeoTIFF file path
    output_filename = os.path.join(output_dir, f'ERA5_{date_str}.tif')
    transform = from_origin(lons[0], lats[0], abs(lons[1] - lons[0]), abs(lats[1] - lats[0]))
    
    # Create GeoTIFF file
    with rasterio.open(output_filename, 'w', driver='GTiff', height=snowcover.shape[0], width=snowcover.shape[1],
                       count=1, dtype='float32', nodata=-9999, crs='EPSG:4326', transform=transform) as dst:
        dst.write(snowcover_binary.astype('float32'), 1)
    
    print(f"GeoTIFF file '{output_filename}' created successfully.")

    # Resampling
    original_path = os.path.join(output_dir, f'ERA5_{date_str}.tif')
    temp_path = os.path.join(output_dir, f'ERA5_{date_str}_temp.tif')
    
    ds = gdal.Open(original_path)
    array = ds.GetRasterBand(1).ReadAsArray()
    
    nan_mask = np.isnan(array)
    array[nan_mask] = np.nan
    
    original_width = ds.RasterXSize
    original_height = ds.RasterYSize
    
    new_width = original_width // 0.1
    new_height = original_height // 0.1
    
    dsRes = gdal.Warp(temp_path, ds, width=new_width, height=new_height, resampleAlg="bilinear", srcNodata="NaN")
    
    # Clipping
    original_path = os.path.join(output_dir, f'ERA5_{date_str}_temp.tif')
    
    
    with rasterio.open(original_path) as src:
        snowcover_data = src.read(1)

        """
        plt.figure(figsize=(10, 5))
        ax = plt.subplot(1, 1, 1)
        show(src, ax=ax)
        plt.title('Original Snow Cover')
        plt.xlabel('Longitude')
        plt.ylabel('Latitude')
        plt.grid(True)
        plt.show()
        """
        shapefile = gpd.read_file(shapefile_path)
        if aoi=="north_italy":
            shapefile = shapefile.to_crs(epsg=4326)        
        clipped_image, clipped_transform = mask(src, shapefile.geometry, crop=True)
        
        threshold = 0.76
        clipped_image = np.where(np.isnan(clipped_image), np.nan, (clipped_image > threshold).astype(int))
        
        clipped_profile = src.profile
        clipped_profile.update({
            "height": clipped_image.shape[1],
            "width": clipped_image.shape[2],
            "transform": clipped_transform,
            "nodata": None
        })
        
        clipped_tiff_path = os.path.join(output_dir, f'Nord_Clipped_ERA5_{date_str}.tif')
        with rasterio.open(clipped_tiff_path, 'w', **clipped_profile) as dst:
            dst.write(clipped_image)
        """
        plt.figure(figsize=(10, 5))
        ax = plt.subplot(1, 1, 1)
        show(clipped_image[0], ax=ax, transform=clipped_transform)
        plt.title('Clipped Snow Cover')
        plt.xlabel('Longitude')
        plt.ylabel('Latitude')
        plt.grid(True)
        plt.show()
        """
        print(f"Clipped Resample saved at {clipped_tiff_path}")
        print(f"resolution in pixels: {clipped_image[0].shape}")

    # Cleanup
    os.remove(temp_path)
    
# Example usage
if __name__ == "__main__":
    date_strs = [ "20240224", "20240225", "20240226"]
    root_dir=""

    aoi_phases=["po_basin","north_italy","north_south_italy"]
    aoi=aoi_phases[1]
    
    for date_str in date_strs:
        process_era_5_snowcover_data(root_dir,date_str,aoi)
    
    #parser = argparse.ArgumentParser(description="Process snowcover data")
    #parser.add_argument("--date_strs",default=["2024-02-23"], nargs="+", help="Date string in the format 'YYYY-MM-DD'")
    #parser.add_argument("--product_dir", default=f"../IRIDE_SNOWCOVER_DATAFUSION/era5_products", help="Product directory path")
    #parser.add_argument("--output_dir", default=f"../IRIDE_SNOWCOVER_DATAFUSION_ERA5", help="Output directory path")
    #args = parser.parse_args()
    #process_snowcover_data(args.date_str, args.product_dir, args.output_dir)

    #for date_str in args.date_strs:
    #    product_dir=os.path.join(args.product_dir, f"{date_str}")
    #    output_dir=os.path.join(args.output_dir, f"{date_str}")
    #    process_snowcover_data(date_str, product_dir, output_dir)
        
    
    
