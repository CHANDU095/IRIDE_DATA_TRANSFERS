import os
import pygrib
import numpy as np
import rasterio
import geopandas as gpd
from rasterio.mask import mask
from rasterio.transform import from_origin
from osgeo import gdal
import matplotlib.pyplot as plt
from rasterio.plot import show

def process_h35_snow_cover_data(date, root_dir,aoi):
    # Extract year, month, and day from the date string
    year = date[:4]
    month = date[4:6]
    day = date[6:]

    # Define the paths
    product_dir = os.path.join(root_dir, f"IRIDE_SNOWCOVER_DATAFUSION_h35/h35_products")
    output_dir = os.path.join(root_dir, f"IRIDE_SNOWCOVER_DATAFUSION_h35/H35_Tiff_files/{year}-{month}-{day}")
    grib_file_path = os.path.join(product_dir, f"{year}-{month}-{day}/h35_{date}_day_merged.grib2")
    output_filename = os.path.join(output_dir, f'H35_{year}-{month}-{day}.tif')


    
    if aoi=="po_basin":
        shapefile_path = os.path.join(root_dir, "utilities/AOI_po_basin_shape/Bacino_fiume_Po.shp")
        gdf=gpd.read_file(shapefile_path)
        lat_min = gdf.geometry.bounds['miny'].min()-0.5
        lat_max = gdf.geometry.bounds['maxy'].max()+0.5
        lon_min = gdf.geometry.bounds['minx'].min()-0.5
        lon_max = gdf.geometry.bounds['maxx'].max()+0.5
    elif aoi =="north_italy":    
        shapefile_path = os.path.join(root_dir, "utilities/AOI_nord_italia_shape/nord_italia.shp")
        gdf=gpd.read_file(shapefile_path)
        lat_min = gdf.geometry.bounds['miny'].min()-0.5
        lat_max = gdf.geometry.bounds['maxy'].max()+0.5
        lon_min = gdf.geometry.bounds['minx'].min()-0.5
        lon_max = gdf.geometry.bounds['maxx'].max()+0.5
    elif aoi =="north_south_italy":
        shapefile_path =os.path.join(root_dir, "utilities/AOI_sud_italia_shape/nord_sud_sicilia.shp")
        gdf=gpd.read_file(shapefile_path)
        lat_min = gdf.geometry.bounds['miny'].min()-0.5
        lat_max = gdf.geometry.bounds['maxy'].max()+0.5
        lon_min = gdf.geometry.bounds['minx'].min()-0.5
        lon_max = gdf.geometry.bounds['maxx'].max()+0.5
    else :
        print("AOI is undefined ....")


    print("ALL paths set")
    # Create output directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Open the GRIB2 file
    grbs = pygrib.open(grib_file_path)

    # Specify the variable name
    variable_name = 'Remotely sensed snow cover'

    # Find the variable in the GRIB2 file
    variable = grbs.select(parameterName=variable_name)[0]

    # Extract the geographical data from the GRIB2 file
    data = variable.data(lat1=lat_min, lat2=lat_max, lon1=lon_min, lon2=lon_max)

    snow_cover = np.array(data[0])
    lats = data[1]
    lons = data[2]

    snow_cover[(snow_cover >= 101) & (snow_cover <= 105)] = np.nan
    threshold = 50.0
    snow_cover_binary = np.where(~np.isnan(snow_cover), snow_cover >= threshold, np.nan)
    snow_cover = snow_cover_binary

    # Create GeoTIFF file
    transform = from_origin(lons[0][0], lats[0][0], abs(lons[0][1] - lons[0][0]), abs(lats[1][0] - lats[0][0]))
    with rasterio.open(output_filename, 'w', driver='GTiff', height=snow_cover.shape[0], width=snow_cover.shape[1],
                       count=1, dtype='float32', nodata=np.nan, crs='EPSG:4326', transform=transform) as dst:
        dst.write(snow_cover.astype('float32'), 1)

    # Reproject and resample GeoTIFF
    ds = gdal.Open(output_filename)
    array = ds.GetRasterBand(1).ReadAsArray()
    nan_mask = np.isnan(array)
    array[nan_mask] = np.nan
    original_width = ds.RasterXSize
    original_height = ds.RasterYSize
    desired_pixel_size = 1000
    new_width = original_width // 0.1
    new_height = original_height // 0.1
    dsRes = gdal.Warp(output_filename, ds, width=new_width, height=new_height, resampleAlg="bilinear",
                      callback=gdal.TermProgress_nocb, srcNodata=np.nan)

    # Delete variables no longer needed
    del grbs, variable, data, snow_cover_binary, ds, array, nan_mask, dsRes

    # Clip GeoTIFF using shapefile
    
    with rasterio.open(output_filename) as src:
        snowcover_data = src.read(1)
        shapefile = gpd.read_file(shapefile_path)
        #transform the crs of the nord_italia shape file only has a diverfy from the rest
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
        clipped_tiff_path = os.path.join(output_dir, f'Clipped_H35_{year}-{month}-{day}.tif')
        with rasterio.open(clipped_tiff_path, 'w', **clipped_profile) as dst:
            dst.write(clipped_image)
        """
        # Plot the clipped GeoTIFF
        plt.figure(figsize=(10, 5))
        ax = plt.subplot(1, 1, 1)
        show(clipped_image[0], ax=ax, transform=clipped_transform)
        
        plt.title('Clipped Snow Cover')
        plt.xlabel('Longitude')
        plt.ylabel('Latitude')
        plt.grid(True)
        plt.show()
        """
        print(f"resolution in pixels: {clipped_image[0].shape}")

    # Delete variables no longer needed
    del shapefile, snowcover_data, clipped_image

    return output_filename, clipped_tiff_path


if __name__ == "__main__":
    # Example usage:

    dates=["20240227"]#,"20240221","20240222","20240223","20240224","20240225","20240226"]
    #date = "20240302"
    
    aoi_phases=["po_basin","north_italy"]
    aoi=aoi_phases[1]
    
    root_dir = ""
    for date in dates:
        process_h35_snow_cover_data(date, root_dir,aoi)
