import glob 
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime



#EXTRACTION DES DONNEES
#
#
#
#

#Définir deux chemins qui seront disponibles dans tout le code
log_file = "log_file.txt"
target_file = "transformed_data.csv"

def extract_from_csv(file_to_extract):
    df = pd.read_csv(file_to_extract)
    return df

def extract_from_json(file_to_extract):
    df = pd.read_json(file_to_extract, lines=True)
    return df

def extract_from_xml(file_to_extract):
    df = pd.DataFrame(columns=["name", "height", "weight"])

    #Transformer le fichier xml en un arbre ElementTree
    tree = ET.parse(file_to_extract)
    #Localiser et stocker la racine pour parcourir l'arbre sans problèmes
    root = tree.getroot()
    for person in root:
        name = person.find("name").text
        height = float(person.find("height").text)
        weight = float(person.find("weight").text)
        df = pd.concat([df, pd.DataFrame([{"name":name, "height":height, "weight":weight}])], ignore_index=True)
        
    return df


def extract():
    extracted_data = pd.DataFrame(columns=["name", "height", "weight"])

    #Pour chaque fichier CSV dans le répertoire
    for csvfile in glob.glob("*.csv"):
        extracted_data = pd.concat([extracted_data, extract_from_csv(csvfile)], ignore_index=True)

    #Pour chaque fichier json dans le répertoire
    for jsonfile in glob.glob("*.json"):
        extracted_data = pd.concat([extracted_data, extract_from_json(jsonfile)], ignore_index=True)

    #Pour chaque fichier XML dans le répertoire
    for xmlfile in glob.glob("*.xml"):
        extracted_data = pd.concat([extracted_data, extract_from_xml(xmlfile)], ignore_index=True)


    return extracted_data

#TRANSFORMATION DES DONNEES
#
#
#
#

def transform(data): 

    #Transformer les données en mesures qu'on utilise
    #1 pouce = 0.0254 mètre
    #Ensuite on prend 2 chiffres après la virgule en arrondissant
    data['height'] = round(data.height * 0.0254, 2) 
    data['weight'] = round(data.weight * 0.45359237, 2) 
    
    return data 

#CHARGEMENT DES DONNEES ET CREATION DE LOG
#
#
#
#

def load_data(target_file, transformed_data): 
    transformed_data.to_csv(target_file) 

def log_progress(message): 
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now() 
    timestamp = now.strftime(timestamp_format) 
    with open(log_file,"a") as f: 
        f.write(timestamp + ',' + message + '\n')


#TEST DU CODE
#
#
#
#


# Log the initialization of the ETL process 
log_progress("ETL Job Started") 
 
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
load_data(target_file,transformed_data) 
 
# Log the completion of the Loading process 
log_progress("Load phase Ended") 
 
# Log the completion of the ETL process 
log_progress("ETL Job Ended")  