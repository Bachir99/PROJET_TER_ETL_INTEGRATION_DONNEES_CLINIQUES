import pandas as pd
import random
import names

def anonymize_names(name, name_map):
    if name not in name_map:
        name_map[name] = names.get_first_name() + ' ' + names.get_last_name()
    return name_map[name]

def anonymize_id(id_number, id_map):
    if id_number not in id_map:
        id_map[id_number] = random.randint(1000000, 9999999)
    return id_map[id_number]

def anonymize_consultant_names(cons_name, cons_name_map):
    if cons_name not in cons_name_map:
        cons_name_map[cons_name] = names.get_first_name() + ' ' + names.get_last_name()
    return cons_name_map[cons_name]

def anonymize_hospital_name(hospital_name, hospital_name_map):
    if hospital_name not in hospital_name_map:
        hospital_name_map[hospital_name] = "Hospital"+str(random.randint(1, 500))
    return hospital_name_map[hospital_name]

def anonymize_dataframe(df, name_map, id_map,hospital_name_map,cons_name_map):
    try:
        df['MEDICAL_RECORD_NAME'] = df['MEDICAL_RECORD_NAME'].apply(anonymize_names, args=(name_map,))
    except KeyError:
        pass
    try:
        df['PATIENT_IDENTIFICATION_NUMBER'] = df['PATIENT_IDENTIFICATION_NUMBER'].apply(anonymize_id, args=(id_map,))
    except KeyError:
        pass
    try :	
        df['HEALTHCARE_HOSPITAL_CLINIC_NAME'] = df['HEALTHCARE_HOSPITAL_CLINIC_NAME'].apply(anonymize_hospital_name, args=(hospital_name_map,))
    except KeyError:
        pass
    try :
    	df['CONSULTANT_NAME'] = df['CONSULTANT_NAME'].apply(anonymize_consultant_names, args=(cons_name_map,))
    except KeyError:
        pass    
    return df

# Remplacez 'exemple_fichier.xlsx' par le nom de votre fichier Excel
fichier_excel = 'Anonymisation.xlsx'

# Lire toutes les feuilles du fichier Excel dans un dictionnaire de DataFrames
xlsx = pd.read_excel(fichier_excel, sheet_name=None)

# Créer un writer pour sauvegarder les DataFrames modifiés
writer = pd.ExcelWriter('fichier_anonymise.xlsx', engine='openpyxl')

# Initialiser les dictionnaires pour les noms et les numéros d'identification
name_map = {}
id_map = {}
hospital_name_map = {}
cons_name_map = {}

# Anonymiser les colonnes 'MEDICAL_RECORD_NAME' et 'PATIENT_IDENTIFICATION_NUMBER' pour chaque feuille
for sheet_name, df in xlsx.items():
    anonymize_dataframe(df, name_map, id_map,hospital_name_map,cons_name_map)
    df.to_excel(writer, sheet_name=sheet_name, index=False)

# Sauvegarder les modifications dans un nouveau fichier Excel
writer.close()

print("Fichier anonymisé avec succès.")
