import pandas as pd
import random
import names
import sys
# Fonction pour anonymiser les noms des patients
def anonymize_names(name, name_map):
    if pd.notnull(name):  # Vérifie si la valeur de la cellule n'est pas NULL
        if name not in name_map:
            name_map[name] = names.get_first_name() + ' ' + names.get_last_name()
        return name_map[name]
    else:
        return name

# Fonction pour anonymiser les numéros d'identification des patients
def anonymize_id(id_number, id_map):
    # Si le numéro d'identification n'est pas dans le dictionnaire de correspondance, en créer un nouveau aléatoirement
    if pd.notnull(id_number):  # Vérifie si la valeur de la cellule n'est pas NULL
        if id_number not in id_map:
            id_map[id_number] = random.randint(1000000, 9999999)
        # Retourne le nouveau numéro d'identification ou le numéro existant dans le dictionnaire
        return id_map[id_number]
    else:
        return id_number
#Même chose pour le nom du consultat et le nom d'hopital

# Fonction pour anonymiser les noms des consultants
def anonymize_consultant_names(cons_name, cons_name_map):
    if pd.notnull(cons_name):  # Vérifie si la valeur de la cellule n'est pas NULL
        if cons_name not in cons_name_map:
            cons_name_map[cons_name] = names.get_first_name() + ' ' + names.get_last_name()
        return cons_name_map[cons_name]
    else :
        return cons_name

# Fonction pour anonymiser les noms des hôpitaux
def anonymize_hospital_name(hospital_name, hospital_name_map):
    if pd.notnull(hospital_name):  # Vérifie si la valeur de la cellule n'est pas NULL
        if hospital_name not in hospital_name_map:
            hospital_name_map[hospital_name] = "Hospital"+str(random.randint(1, 500)) #le nom de l'hopital va être hospital1,hospital2 .... ?
        return hospital_name_map[hospital_name]
    else :
        return hospital_name
    
# Fonction qui regroupe tout
def anonymize_dataframe(df, name_map, id_map, encounter_id, mr_no_id, doctor_id, hospital_name_map,cons_name_map):
    # Anonymiser la colonne 'MEDICAL_RECORD_NAME' en utilisant la fonction "anonymize_names"
    try:
        df['PATIENT_NAME_ENGLISH'] = df['PATIENT_NAME_ENGLISH'].apply(anonymize_names, args=(name_map,))
    except KeyError:
        pass # si la colonne n'existe pas dans la feuille on passe
   # Anonymiser la colonne 'PATIENT_IDENTIFICATION_NUMBER' en utilisant la fonction "anonymize_id"
    try:
        df['MR_NO'] = df['MR_NO'].apply(anonymize_id, args=(mr_no_id,))
    except KeyError:
        pass # si la colonne n'existe pas dans la feuille on passe
    
    try:
        df['PATIENTID'] = df['PATIENTID'].apply(anonymize_id, args=(id_map,))
    except KeyError:
        pass # si la colonne n'existe pas dans la feuille on passe
    try:
        df['ENCOUNTERID'] = df['ENCOUNTERID'].apply(anonymize_id, args=(encounter_id,))
    except KeyError:
        pass # si la colonne n'existe pas dans la feuille on passe
    try:
        df['DOCTOR_ID'] = df['DOCTOR_ID'].apply(anonymize_id, args=(doctor_id,))
    except KeyError:
        pass # si la colonne n'existe pas dans la feuille on passe   
    try:
        df['PRIMARY_SURGEON_ID'] = df['PRIMARY_SURGEON_ID'].apply(anonymize_id, args=(doctor_id,))
    except KeyError:
        pass
    try:
        df['SURGEON2_ID'] = df['SURGEON2_ID'].apply(anonymize_id, args=(doctor_id,))
    except KeyError:
        pass  
    try :	
        df['CLINIC'] = df['CLINIC'].apply(anonymize_hospital_name, args=(hospital_name_map,))
    except KeyError:
        pass
    try :
    	df['DOCTOR_NAME'] = df['DOCTOR_NAME'].apply(anonymize_consultant_names, args=(cons_name_map,))
    except KeyError:
        pass
    try :
    	df['DOCTOR_NAME.1'] = df['DOCTOR_NAME.1'].apply(anonymize_consultant_names, args=(cons_name_map,))
    except KeyError:
        pass
    try :
    	df['CONSULTANT'] = df['CONSULTANT'].apply(anonymize_consultant_names, args=(cons_name_map,))
    except KeyError:
        pass
    try :
    	df['PRIMARY_SURGEON_NAME'] = df['PRIMARY_SURGEON_NAME'].apply(anonymize_consultant_names, args=(cons_name_map,))
    except KeyError:
        pass
    try :
    	df['SURGEON2_NAME'] = df['SURGEON2_NAME'].apply(anonymize_consultant_names, args=(cons_name_map,))
    except KeyError:
        pass
    return df

# Remplacez 'exemple_fichier.xlsx' par le nom de votre fichier Excel
fichier_excel = 'CIUSS_TKFH_2019.xlsx' #LE NOM DU FICHIER A MODIFIER ICI !!

# Lire toutes les feuilles du fichier Excel dans un dictionnaire de DataFrames
xlsx = pd.read_excel(fichier_excel, sheet_name=None)

# Créer un writer pour sauvegarder les DataFrames modifiés
#writer = pd.ExcelWriter('fichier_anonymise.xlsx', engine='openpyxl')

# Initialiser les dictionnaires pour les noms et les numéros d'identification
name_map = {}
id_map = {}
hospital_name_map = {}
cons_name_map = {}
encounter_id={}
mr_no_id={}
doctor_id={}
#fichier_excel_anonymise = 'CIUSS_TKFH_2019_anonymise.xlsx'

# Anonymiser les colonnes 'MEDICAL_RECORD_NAME' et 'PATIENT_IDENTIFICATION_NUMBER' pour chaque feuille
for sheet_name, df in xlsx.items():
    # Nom du fichier Excel anonymisé pour cette feuille
    fichier_excel_anonymise = f'CIUSS_TKFH_2019_anonymise_{sheet_name}.xlsx'
    anonymize_dataframe(df, name_map, id_map, encounter_id, mr_no_id, doctor_id, hospital_name_map, cons_name_map).to_excel(fichier_excel_anonymise, index=False)


print("Fichier anonymisé avec succès.")
