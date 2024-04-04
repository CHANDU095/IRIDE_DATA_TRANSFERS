from skimage.transform import resize
import numpy as np
import rasterio
import os
from rasterio.enums import Resampling
from rasterio.warp import reproject
import geopandas as gpd
from rasterio.mask import mask
from shapely.geometry import shape
import numpy.ma as ma
import matplotlib.pyplot as plt

def fusion_process_snowcover_data(date_str, root_dir,aoi):
    year = date_str[:4]
    month = date_str[4:6]
    day = date_str[6:]

    era5_folder = os.path.join(root_dir, f"IRIDE_SNOWCOVER_DATAFUSION_ERA5/Final_era5_Tifs/{year}-{month}-{day}/")
    h35_folder = os.path.join(root_dir, f"IRIDE_SNOWCOVER_DATAFUSION_h35/H35_Tiff_files/{year}-{month}-{day}/")
    sentinel_folder = os.path.join(root_dir, f"IRIDE_SNOWCOVER_DATAFUSION_S3/Final_SEN3_Tiffs/{date_str}/")
    final_folder = os.path.join(root_dir, f"IRIDE_SNOWCOVER_DATAFUSION_PRODUCTS/{year}-{month}-{day}/")

    if not os.path.exists(final_folder):
        os.makedirs(final_folder)

    if aoi=="po_basin":
        shapefile_path=shapefile_path = os.path.join(root_dir, "utilities/AOI_po_basin_shape/Bacino_fiume_Po.shp")
    elif aoi =="north_italy":    
        shapefile_path = os.path.join(root_dir, "utilities/AOI_nord_italia_shape/nord_italia.shp")
    elif aoi =="north_south_italy":
        shapefile_path =os.path.join(root_dir, "utilities/AOI_sud_italia_shape/nord_sud_sicilia.shp")
    else :
        print("AOI is undefined ....")
        
    era5_tiff_path = os.path.join(era5_folder, f"Nord_Clipped_ERA5_{year}-{month}-{day}.tif")
    h35_tiff_path = os.path.join(h35_folder, f"Clipped_H35_{year}-{month}-{day}.tif")
    sentinel3_tiff_path = os.path.join(sentinel_folder, f"Clipped_sentinel_{date_str}.tif")
    output_tiff_path1 = os.path.join(final_folder, f"FUSION_{date_str}.tif")

    with rasterio.open(sentinel3_tiff_path) as sentinel3_src:
        sentinel3_profile = sentinel3_src.profile
        sentinel3_band = sentinel3_src.read(1)

        #Run to avoid setinel data
        sentinel3_band[:] = np.nan

        h35_data = None
        era5_data = None

        if os.path.exists(h35_tiff_path):
            with rasterio.open(h35_tiff_path) as h35_src:
                h35_band = h35_src.read(1, masked=True)
                h35_band_resized = resize(h35_band, sentinel3_band.shape, anti_aliasing=True)
                if h35_data is None:
                    h35_data = h35_band_resized
                else:
                    h35_data = np.where(np.isnan(h35_data), h35_band_resized, h35_data)

        if os.path.exists(era5_tiff_path):
            with rasterio.open(era5_tiff_path) as era5_src:
                era5_band = era5_src.read(1, masked=True)
                era5_band_resized = resize(era5_band, sentinel3_band.shape, anti_aliasing=True)
                if era5_data is None:
                    era5_data = era5_band_resized
                else:
                    era5_data = np.where(np.isnan(era5_data), era5_band_resized, era5_data)

        if h35_data is not None:
            sentinel3_band = np.where(np.isnan(sentinel3_band), h35_data, sentinel3_band)

        if era5_data is not None:
            for i in range(sentinel3_band.shape[0]):
                for j in range(sentinel3_band.shape[1]):
                    if np.isnan(sentinel3_band[i, j]) and not np.isnan(era5_data[i, j]):
                        sentinel3_band[i, j] = era5_data[i, j]

        profile = sentinel3_profile

        with rasterio.open(output_tiff_path1, 'w', **profile) as dst:
            dst.write(sentinel3_band, 1)

    tiff_path = output_tiff_path1

    
            
    with rasterio.open(tiff_path) as src:
        snowcover_data_buf = src.read(1)
        north_lakes_shapefile_path=os.path.join(root_dir, "utilities/AOI_nord_italia_lakes/Northern_Italy_lakes_2km.shp")
            
        lakes_gdf = gpd.read_file(north_lakes_shapefile_path)
        lakes_clip_geometry = lakes_gdf.geometry.iloc[0]
        lakes_clipped_data, lakes_clipped_transform = mask(src, [shape(lakes_clip_geometry)], invert=True, filled=True, nodata=0)
        # Update the metadata for the clipped TIFF
        lakes_clipped_meta = src.meta
        lakes_clipped_meta.update({
            'height': lakes_clipped_data.shape[1],
            'width': lakes_clipped_data.shape[2],
            'transform': lakes_clipped_transform,
        })
        with rasterio.open(tiff_path, 'w', **lakes_clipped_meta) as dst:
            dst.write(lakes_clipped_data)

    with rasterio.open(tiff_path) as src:
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
            "nodata": -999
        })

        print("Clipping for the northern Lakes..")
        clipped_tiff_path = os.path.join(final_folder, f"IRIDE-S_S5-02-05_{date_str}_V0.tif")
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

# Example usage
if __name__ == "__main__":
    #date_strs = ["20240226","20240224","20240223"]
    date_strs = ["20240306"]
    root_dir = ""
    aoi_phases=["po_basin","north_italy"]
    aoi=aoi_phases[1]
    for date_str in date_strs:
        fusion_process_snowcover_data(date_str, root_dir,aoi)
