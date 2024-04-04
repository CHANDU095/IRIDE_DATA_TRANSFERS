#!/bin/bash

# Loop through each TIFF file in the folder
for tiff_file in *.tif; do
    # Define the output filename
    output_file="${tiff_file%.tif}_processed.tif"
    # Apply gdal_translate
    gdal_translate -a_nodata -999 "$tiff_file" "$output_file"
    # Replace the original TIFF file with the processed one
    mv "$output_file" "$tiff_file"
done
