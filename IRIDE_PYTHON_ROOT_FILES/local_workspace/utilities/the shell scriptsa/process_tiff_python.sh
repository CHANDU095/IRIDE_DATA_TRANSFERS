#!/bin/bash

# Function to process a single TIFF file
process_tiff() {
    input_file="$1"
    output_file="$2"
    python process_tiff.py "$input_file" "$output_file"
    mv "$output_file" "$input_file"
}

# Directory containing TIFF files
input_folder="."

# Loop through each TIFF file in the folder
for file_name in "$input_folder"/*.tif; do
    output_file="${file_name%.tif}_processed.tif"
    process_tiff "$file_name" "$output_file"
    
done
