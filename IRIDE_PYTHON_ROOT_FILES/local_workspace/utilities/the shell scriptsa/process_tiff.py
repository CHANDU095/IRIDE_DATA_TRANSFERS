import numpy as np
from osgeo import gdal
import sys

def process_nan_values_tiff(input_file, output_file):
    # Open the input GeoTIFF file
    dataset = gdal.Open(input_file, gdal.GA_ReadOnly)
    if dataset is None:
        print(f"Could not open the input file: {input_file}")
        return
    
    # Read raster data
    raster = dataset.ReadAsArray()

    # Replace NaN values with -999
    raster[np.isnan(raster)] = -999

    # Write the modified raster to output GeoTIFF file
    driver = gdal.GetDriverByName("GTiff")
    output_dataset = driver.Create(output_file, dataset.RasterXSize, dataset.RasterYSize, 1, gdal.GDT_Float32)
    output_dataset.SetGeoTransform(dataset.GetGeoTransform())
    output_dataset.SetProjection(dataset.GetProjection())
    output_dataset.GetRasterBand(1).WriteArray(raster)

    # Close datasets
    dataset = None
    output_dataset = None
    print(f"Processed: {input_file}")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python process_tiff.py input_tiff output_tiff")
        sys.exit(1)

    input_tiff = sys.argv[1]
    output_tiff = sys.argv[2]

    process_nan_values_tiff(input_tiff, output_tiff)

