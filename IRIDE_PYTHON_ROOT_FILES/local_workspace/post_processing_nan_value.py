import numpy as np
from osgeo import gdal
import subprocess
import os

def process_nan_values(input_file):
    # Open the input GeoTIFF file
    dataset = gdal.Open(input_file, gdal.GA_Update)
    if dataset is None:
        print(f"Could not open the input file: {input_file}")
        return
    
    # Read raster data
    raster = dataset.ReadAsArray()

    # Replace NaN values with -999
    raster[np.isnan(raster)] = -999

    # Write the modified raster back to the input GeoTIFF file
    dataset.GetRasterBand(1).WriteArray(raster)

    # Close dataset
    dataset = None

    # Apply gdal_translate to set the NoData value
    temp_output_tiff = "temp_processed.tif"
    subprocess.run(["gdal_translate", "-a_nodata", "-999", input_file, temp_output_tiff])

    # Replace the original GeoTIFF file with the processed one
    os.replace(temp_output_tiff, input_file)

    print(f"Processing completed for: {input_file}")

# Example usage:
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python process_tiff.py input_tiff")
        sys.exit(1)

    input_tiff = sys.argv[1]
    process_nan_values(input_tiff)
