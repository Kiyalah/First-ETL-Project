import pandas as pd
import glob 
import xml.etree.ElementTree as ET
from datetime import datetime

log_file = "log_file.txt"
target_file = "transformed_data.csv"

def extract_csv_file(csvfile):
    df = pd.read_csv(csvfile)
    return df

def extract_json_file(jsonfile):
    df = pd.read_json(jsonfile, lines=True)
    return df

def extract_xml_file(xmlfile):
    df = pd.DataFrame(columns=["car_model", "year_of_manufacture", "price", "fuel"])
    tree = ET.parse(xmlfile)
    root = tree.getroot()
    for row in root:
        car_model = row.find("car_model").text
        year_of_manufacture = row.find("year_of_manufacture").text
        price = row.find("price").text
        fuel = row.find("fuel").text
        df = pd.concat([df, pd.DataFrame([{"car_model":car_model, "year_of_manufacture":year_of_manufacture, "price":price, "fuel":fuel}])], ignore_index=True)

    return df

def extract():
    extracted_data = pd.DataFrame(columns=["car_model", "year_of_manufacture", "price", "fuel"])

    for csvfile in glob.glob("*.csv"):
        extracted_data = pd.concat([extracted_data, extract_csv_file(csvfile)])

    for jsonfile in glob.glob("*.json"):
        extracted_data = pd.concat([extracted_data, extract_json_file(jsonfile)])

    for xmlfile in glob.glob("*.xml"):
        extracted_data = pd.concat([extracted_data, extract_xml_file(xmlfile)])
    
    return extracted_data

def transform_data(data):
    data["price"] = round(data.price, 2)
    return data

def load_data(target_file, transformed_data):
    transformed_data.to_csv(target_file, index=False)

def log_progress(message): 
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now() 
    timestamp = now.strftime(timestamp_format) 
    with open(log_file,"a") as f: 
        f.write(timestamp + ',' + message + '\n')

log_progress("ETL Job Started") 
 
log_progress("Extract phase Started") 
extracted_data = extract() 
log_progress("Extract phase Ended") 
 
log_progress("Transform phase Started") 
transformed_data = transform_data(extracted_data) 
print("Transformed Data") 
print(transformed_data) 
log_progress("Transform phase Ended") 
 
log_progress("Load phase Started") 
load_data(target_file,transformed_data) 
log_progress("Load phase Ended") 
 
log_progress("ETL Job Ended")

    