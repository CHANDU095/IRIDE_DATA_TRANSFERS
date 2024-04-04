import os
import argparse
import gzip
import shutil
import multiprocessing
from ftplib import FTP

def download_h35_data(target_day, download_folder='.'):
    if not os.path.exists(download_folder):
        os.makedirs(download_folder)

    files_and_folders = os.listdir(download_folder)
    if files_and_folders:
        for item in files_and_folders:
            full_path = os.path.join(download_folder, item)
            os.remove(full_path) if os.path.isfile(full_path) else shutil.rmtree(full_path)
        print(f"Files and folders in {download_folder} deleted.")
    else:
        print(f"{download_folder} is empty.")

    ftp = FTP('ftphsaf.meteoam.it')  
    ftp.login(user='imageprocessinglab.uniroma2@gmail.com', passwd='EOlab2023')   
    ftp.cwd('/h35/h35_cur_mon_data')  

    file_list = []
    ftp.dir(file_list.append)

    matching_files = [file_info.split()[-1] for file_info in file_list if target_day in file_info]
    
    if not matching_files:
        print(f"No data available for the specified day: {target_day}")
        return
    
    print(f"Downloading data for {target_day} into folder: {download_folder}...")
    for file_name in matching_files:
        file_path = os.path.join(download_folder, file_name)
        print(file_name)
        
        with open(file_path, 'wb') as f:
            ftp.retrbinary('RETR ' + file_name, f.write)
        
        with gzip.open(file_path, 'rb') as f_in, open(file_path[:-3], 'wb') as f_out:
            f_out.write(f_in.read())
            
        os.remove(file_path)

    ftp.quit()

def download_data_for_date(date_str, root_path):
    day = date_str[:2]
    month = date_str[3:5]
    year = date_str[6:]
    target_day = f"{year+month+day}"
    target_day_str = f"{year}-{month}-{day}"
    
    h35_download_folder = os.path.join(root_path, 'IRIDE_SNOWCOVER_DATAFUSION_h35/h35_products', target_day_str)
    download_h35_data(target_day, h35_download_folder)

def h35_data_download_parallel(dates, root_path):
    processes = []
    for date_str in dates:
        year = int(date_str[:4])
        month = int(date_str[4:6])
        day = int(date_str[6:])
        date_str=f"{day:02d}_{month:02d}_{year}"
        p = multiprocessing.Process(target=download_data_for_date, args=(date_str, root_path))
        processes.append(p)
        p.start()

    for p in processes:
        p.join()

def main():
    root_path = ""
    #dates =[ "20240224", "20240225", "20240226", "20240227", "20240228", "20240223", "20240222"] #["20_02_2024", "21_02_2024", "22_02_2024" ]
    #dates =["20240125", "20240128", "20240131", "20240202", "20240205", "20240209", "20240213", "20240214", "20240215", "20240217", "20240218", "20240220", "20240221", "20240225"]
    #dates =[ "20240213", "20240214", "20240215", "20240217", "20240218", "20240220", "20240221", "20240225"]
    dates =[ "20240205"]
    dates =[
        "20240318","20240317","20240316","20240315","20240314","20240313","20240312","20240311",
        "20240310","20240309","20240308","20240307","20240306","20240305","20240304","20240303",
        "20240302","20240301","20240229","20240228", "20240227", "20240226", "20240225","20240224",
        "20240223", "20240222","20240221","20240220", "20240219", "20240218", "20240217","20240216",
        "20240215", "20240214","20240213","20240212", "20240211", "20240210", "20240209","20240208",
        "20240207", "20240206","20240205","20240204", "20240203", "20240202", "20240201","20240131",
        "20240130", "20240129","20240128","20240127", "20240126", "20240125", "20240124"
    ]

    h35_data_download_parallel(dates[0:6], root_path)
    
    #parser = argparse.ArgumentParser(description='Download H35 data parallelly.')
    #parser.add_argument('--day_list', metavar='day_list', type=str, nargs='+',help='List of days ',default=["24_02_2024", "25_02_2024", "26_02_2024"])
    #parser.add_argument('--root_path', metavar='root_path', type=str,default="IRIDE_SNOWCOVER_DATAFUSION_h35/",help='Root path')
    #args = parser.parse_args()
    #h35_data_download_parallel( args.day_list, args.root_path)

if __name__ == "__main__":
    main()
