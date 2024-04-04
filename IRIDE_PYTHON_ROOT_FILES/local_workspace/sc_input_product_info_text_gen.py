
import os
from datetime import datetime




# Function to extract date and time from the filename
def extract_date_time_from_sentinel_product_info(filename):
    parts = filename.split('____')
    date_str = parts[1][:8]  # Extracting the date part
    time_str = parts[1][9:15]  # Extracting the time part
    return datetime.strptime(date_str + time_str, '%Y%m%d%H%M%S')

# Function to concatenate text files for a given date
def concatenate_files_total_input_info_file(root_dir,date):
   # Read content from L1 and L2 folders
    l1_file_path = f"IRIDE_SNOWCOVER_DATAFUSION_S3/sentinel_product_info/S3_L1_OLCI/in_product_names_{date}.txt"
    l2_file_path = f"IRIDE_SNOWCOVER_DATAFUSION_S3/sentinel_product_info/S3_L2_OLCI/in_product_names_{date}.txt"

    l1_file_path = os.path.join(root_dir,l1_file_path)
    l2_file_path = os.path.join(root_dir,l2_file_path)

    print(f"Info_PATHS  {l1_file_path} and {l2_file_path}")
    
    l1_content = ""
    l2_content = ""
    if os.path.exists(l1_file_path) and os.path.exists(l2_file_path):
        with open(l1_file_path, "r") as l1_file:
            l1_content=l1_file.readlines()
        
        with open(l2_file_path, "r") as l2_file:
            l2_content=l2_file.readlines()
    
        for line_1 in l1_content:
            parts_1 = line_1.strip().split('_')
            level_1 = parts_1[2]
            date_time_1 = extract_date_time_from_sentinel_product_info(line_1)   
            formatted_date_time_1 = date_time_1.strftime('%d/%m/%YT%H:%M:%S')  # Corrected the format string
            #formatted_date_time_1 = date_time_1.strftime('%Y-%m-%DT%H:%M:%S')
            l1_info=f'-[IN-S5-02-02] Sentinel-3-OLCI L{level_1} EFR; {formatted_date_time_1}; 300m'
    
        for line_2 in l2_content:
            parts_2 = line_2.strip().split('_')
            level_2 = parts_2[2]
            date_time_2 = extract_date_time_from_sentinel_product_info(line_2)   
            formatted_date_time_2 = date_time_2.strftime('%d/%m/%YT%H:%M:%S')  # Corrected the format string
            #formatted_date_time_2 = date_time_2.strftime('%Y-%m-%DT%H:%M:%S')  # Corrected the format string
            l2_info=f'-[IN-S5-02-02] Sentinel-3-OLCI L{level_2} LFR; {formatted_date_time_2}; 300m'
    
    
    
        formatted_date_time_era_h35 = date_time_2.strftime('%d/%m/%YT%H:00:00')
        #formatted_date_time_era_h35 = date_time_2.strftime('%Y-%m-%DT%H:%M:%S')
        H35_info=f'-[IN-S5-02-15] HSAF/H35; {formatted_date_time_era_h35}; 1113m'
        ERA5_info=f'-[IN-S5-02-20] ERA5-LAND-HOURLY-DATA/SNOWCOVER; {formatted_date_time_era_h35}; 9000m' #to be changed
    
        
        # Concatenate content
        concatenated_content ="Input data :\n" +l1_info + "\n" + l2_info+ "\n" + H35_info+ "\n" + ERA5_info
    
        # Write concatenated content to new file
        output_folder =os.path.join(root_dir, f"Input_products_info_text_files")
        os.makedirs(output_folder, exist_ok=True)
        output_file_path = os.path.join(output_folder, f"S5-02-05_inputs_{date}.txt")
        
        with open(output_file_path, "w") as output_file:
            output_file.write(concatenated_content)

    else:
        print("One or both of the Sentinel product info text files do not exist.")




if __name__ == "__main__" :
    # List dates
    dates = ["20240229"]
    
    root_dir="IRIDE_PYTHON_FILES_ROOT/"
    # Process files for each date
    for date in dates:
        concatenate_files_total_input_info_file(root_dir,date)
    
    print("Files processed successfully.")