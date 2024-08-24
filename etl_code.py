import glob
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime
import requests
import zipfile
import os

# Introducing paths
log_file = "log_file.txt"
target_file = "transformed_data.csv"
zip_url = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Lab%20-%20Extract%20Transform%20Load/data/source.zip"
download_path = "downloaded_file.zip"
extract_folder = "extracted_files"

# Download and unzip the file
def download_and_unzip(url, download_path, extract_folder):
    # Download the zip file
    response = requests.get(url)
    with open(download_path, 'wb') as f:
        f.write(response.content)
    
    # Unzip the file
    with zipfile.ZipFile(download_path, 'r') as zip_ref:
        zip_ref.extractall(extract_folder)

# EXTRACTION
# Extracting CSV files
def extract_from_csv(file_to_process):
    dataframe = pd.read_csv(file_to_process)
    return dataframe

# Extracting JSON files
def extract_from_json(file_to_process):
    dataframe = pd.read_json(file_to_process, lines=True)
    return dataframe

# Extracting XML files
def extract_from_xml(file_to_process):
    dataframe = pd.DataFrame(columns = ['name','height','weight'])
    tree = ET.parse(file_to_process)
    root = tree.getroot()
    for person in root:
        name = person.find('name').text
        height = float(person.find('height').text)
        weight = float(person.find('weight').text)
        dataframe = pd.concat([dataframe, pd.DataFrame([{'name': name, 'height': height, 'weight': weight}])], ignore_index=True)
    return dataframe

# Create an empty data frame to hold extracted data
def extract():
    extracted_data = pd.DataFrame(columns=['name', 'height', 'weight'])
    # Identifying function to call based on filetype
    for csvfile in glob.glob(f"{extract_folder}/*.csv"):
        extracted_data = pd.concat([extracted_data, extract_from_csv(csvfile)], ignore_index=True)
    for jsonfile in glob.glob(f"{extract_folder}/*.json"):
        extracted_data = pd.concat([extracted_data, extract_from_json(jsonfile)], ignore_index=True)
    for xmlfile in glob.glob(f"{extract_folder}/*.xml"):
        extracted_data = pd.concat([extracted_data, extract_from_xml(xmlfile)], ignore_index=True)
    return extracted_data

# TRANSFORMATION
def transform(data):
    "Convert inches to metres and round off to two decimals 1 inch is 0.0254 metres"
    data['height'] = round(data.height * 0.0254, 2)

    "Convert pounds to kilograms and round off to two decimals 1 pound is 0.45359237"
    data['weight'] = round(data.weight * 0.45359237, 2)

    return data 

# LOADING AND LOGGING
# Loading data 
def load_data(target_file, transformed_data):
    transformed_data.to_csv(target_file, index=False)

# Logging progress 
def log_progress(message):
    timestamp_format = '%Y-%m-%d %H:%M:%S'  # Fixed timestamp format
    now = datetime.now()  # Get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open(log_file, "a") as f: 
        f.write(timestamp + ',' + message + '\n')

# Log the initialization of the ETL process 
log_progress("ETL Job Started")

# Download and unzip the files
log_progress("Download and unzip phase Started")
download_and_unzip(zip_url, download_path, extract_folder)
log_progress("Download and unzip phase Ended")

# Log the beginning of the Extraction process 
log_progress("Extract phase Started") 
extracted_data = extract() 

# Log the completion of the Extraction process 
log_progress("Extract phase Ended") 

# Log the beginning of the Transformation process 
log_progress("Transform phase Started") 
transformed_data = transform(extracted_data) 
print("Transformed Data") 
print(transformed_data) 

# Log the completion of the Transformation process 
log_progress("Transform phase Ended") 

# Log the beginning of the Loading process 
log_progress("Load phase Started") 
load_data(target_file, transformed_data) 

# Log the completion of the Loading process 
log_progress("Load phase Ended") 

# Log the completion of the ETL process 
log_progress("ETL Job Ended")
