import pandas as pd
import random
import names

# Fonction pour anonymiser les noms des patients
def anonymize_names(name, name_map):
    # Si le nom n'est pas dans le dictionnaire de correspondance, en créer un nouveau en utilisant la bibliothèque "names"
    if name not in name_map:
        name_map[name] = names.get_first_name() + ' ' + names.get_last_name()
    # Retourne le nouveau nom ou le nom existant dans le dictionnaire
    return name_map[name]

# Fonction pour anonymiser les numéros d'identification des patients
def anonymize_id(id_number, id_map):
    # Si le numéro d'identification n'est pas dans le dictionnaire de correspondance, en créer un nouveau aléatoirement
    if id_number not in id_map:
        id_map[id_number] = random.randint(1000000, 9999999)
    # Retourne le nouveau numéro d'identification ou le numéro existant dans le dictionnaire
    return id_map[id_number]

#Même chose pour le nom du consultat et le nom d'hopital

# Fonction pour anonymiser les noms des consultants
def anonymize_consultant_names(cons_name, cons_name_map):
    if cons_name not in cons_name_map:
        cons_name_map[cons_name] = names.get_first_name() + ' ' + names.get_last_name()
    return cons_name_map[cons_name]

# Fonction pour anonymiser les noms des hôpitaux
def anonymize_hospital_name(hospital_name, hospital_name_map):
    if hospital_name not in hospital_name_map:
        hospital_name_map[hospital_name] = "Hospital"+str(random.randint(1, 500)) #le nom de l'hopital va être hospital1,hospital2 .... ?
    return hospital_name_map[hospital_name]

# Fonction qui regroupe tout
def anonymize_dataframe(df, name_map, id_map,hospital_name_map,cons_name_map):
    # Anonymiser la colonne 'MEDICAL_RECORD_NAME' en utilisant la fonction "anonymize_names"
    try:
        df['MEDICAL_RECORD_NAME'] = df['MEDICAL_RECORD_NAME'].apply(anonymize_names, args=(name_map,))
    except KeyError:
        pass # si la colonne n'existe pas dans la feuille on passe
   # Anonymiser la colonne 'PATIENT_IDENTIFICATION_NUMBER' en utilisant la fonction "anonymize_id"
    try:
        df['PATIENT_IDENTIFICATION_NUMBER'] = df['PATIENT_IDENTIFICATION_NUMBER'].apply(anonymize_id, args=(id_map,))
    except KeyError:
        pass # si la colonne n'existe pas dans la feuille on passe
    #Pareil pour les 2 autres
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
fichier_excel = 'Anonymisation.xlsx' #LE NOM DU FICHIER A MODIFIER ICI !!

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
    anonymize_dataframe(df, name_map, id_map,hospital_name_map,cons_name_map)  #On applique la méthod
    df.to_excel(writer, sheet_name=sheet_name, index=False) #one convertit en excel le DataFrame résultat

# Sauvegarder les modifications dans un nouveau fichier Excel
writer.close()

print("Fichier anonymisé avec succès.")
