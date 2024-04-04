import os
import argparse
from multiprocessing import Process
import shutil

import cdsapi

def download_era5_data(date_string, save_folder):

    if not os.path.exists(save_folder):
        os.makedirs(save_folder)
    try:
        files_and_folders = os.listdir(save_folder)
        if files_and_folders:
            for item in files_and_folders:
                full_path = os.path.join(save_folder, item)
                os.remove(full_path) if os.path.isfile(full_path) else shutil.rmtree(full_path)
            print(f"Files and folders in {save_folder} deleted.")
        else:
            print(f"{save_folder} is empty.")

        file_path = os.path.join(save_folder, f'era5_{date_string}.nc')

        # Replace 'YOUR_UID' and 'YOUR_API_KEY' with your actual UID and API key
        UID = '275818'  # SOSTITUIRE
        API_KEY = '767ac954-78f6-4ad6-a38b-4a444a1c82b9'

        c = cdsapi.Client()
        c.url = "https://cds.climate.copernicus.eu/api/v2"
        c.key = f"{UID}:{API_KEY}"

        # Extract year, month, and day from the date string
        year, month, day = date_string.split('-')

        # Make the request to retrieve data
        response = c.retrieve(
            'reanalysis-era5-land',
            {
                'variable': 'snow_cover',
                'year': year,
                'month': month,
                'day': day,
                'time': [
                    '00:00', '01:00', '02:00',
                    '03:00', '04:00', '05:00',
                    '06:00', '07:00', '08:00',
                    '09:00', '10:00', '11:00',
                    '12:00', '13:00', '14:00',
                    '15:00', '16:00', '17:00',
                    '18:00', '19:00', '20:00',
                    '21:00', '22:00', '23:00',
                ],
                'format': 'netcdf',
            },
            file_path
        )

        print(f"File for {date_string} downloaded successfully.")

    except Exception as e:
        print(f"An error occurred while downloading the file for {date_string}: {str(e)}")

def era5_data_download_parallel(date_list, root_path):
    era5_products_folder = os.path.join(root_path,'IRIDE_SNOWCOVER_DATAFUSION_ERA5/era5_products/')
    processes = []

    for date in date_list:
        # Extract year, month, and day from the date string
        year = date[:4]
        month = date[4:6]
        day = date[6:]
        
        target_day_string = f'{year}-{month}-{day}'
        era5_download_folder = os.path.join(era5_products_folder, target_day_string)
        
        # Create folder if it doesn't exist
        os.makedirs(era5_download_folder, exist_ok=True)
        
        # Start a new process for downloading data
        p = Process(target=download_era5_data, args=(target_day_string, era5_download_folder))
        p.start()
        processes.append(p)
    
    # Wait for all processes to finish
    for p in processes:
        p.join()





# Example usage
if __name__ == "__main__":
    
    
    day_list =[
        "20240319","20240318","20240317","20240316","20240315","20240314","20240313","20240312",
        "20240311","20240310","20240309","20240308","20240307","20240306","20240305","20240304",
        "20240303","20240302","20240301","20240229","20240228", "20240227", "20240226", "20240225",
        "20240224", "20240223", "20240222","20240221","20240220", "20240219", "20240218", "20240217",
        "20240216", "20240215", "20240214","20240213","20240212", "20240211", "20240210", "20240209",
        "20240208", "20240207", "20240206","20240205","20240204", "20240203", "20240202", "20240201",
        "20240131", "20240130", "20240129","20240128","20240127", "20240126", "20240125", "20240124"
    ]

    day_list =["20240316","20240317","20240318"]
    day_list =["20240321","20240322","20240323","20240324","20240325","20240326"]#,"20240320","20240319"]
    day_list =["20240325","20240326","20240327","20240328","20240329"]

    root_path = ""
    era5_data_download_parallel(day_list[:], root_path)


    #parser = argparse.ArgumentParser(description='Download ERA5 data parallelly.')
    #parser.add_argument('day_list', metavar='day_list', type=str, nargs='+',help='List of days ',default=["24_02_2024", "25_02_2024", "26_02_2024"])
    #parser.add_argument('root_path', metavar='root_path', type=str,default="IRIDE_SNOWCOVER_DATAFUSION/",help='Root path')
    #args = parser.parse_args()
    #download_data_parallel( args.day_list, args.root_path)
    
    
    
