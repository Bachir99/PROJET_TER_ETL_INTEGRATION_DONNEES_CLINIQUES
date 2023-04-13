import sys
import os
import pandas as pd
import re
import datetime
from datetime import datetime, timedelta
import numpy as np

def v_today1_v_date_of_birth1_v_date_of_death(df):
    today = datetime.now()
    max_dob = today - timedelta(days=125 * 365)

    def check_dates(row):
        dob = pd.to_datetime(row['DateOfBirth'], errors='coerce')
        dod = pd.to_datetime(row['DateofDeath'], errors='coerce')
        
        if dob > today or dod > today:
            return False

        if dob is not pd.NaT and dob < max_dob:
            return False

        if dod is not pd.NaT and dob is not pd.NaT and dod < dob:
            return False

        return True

    df = df[df.apply(check_dates, axis=1)]

    return df

def D_patientDeceased(row):
    if pd.isna(row['PatientDeceased']):
        if pd.notna(row['DateofDeath']):
            return 'Oui'
    return row['PatientDeceased']



def V_NotNull1(row):
    if pd.isna(row['Hospital']) or row['Hospital'] == '':
        return False
    return True

def enlever_all_null_colonnes(df):
    for col in df.columns:
        if df[col].isna().all():
            df.drop(col, axis=1, inplace=True)


def V_length50(row, warnings_list):
    if len(str(row['PatientNumber'])) > 50:
        row_copy = row.copy()
        row_copy['Avertissement'] = 'V-length50'
        warnings_list.append(row_copy)


def V_length100(df, row, warnings_list):
    for col in df.columns :
        if col in ['FathersName', 'FathersPreName', 'PlaceOfBirth']:
            if len(str(row[col])) > 100:
                row_copy = row.copy()
                row_copy['Avertissement'] = 'V-length50'
                warnings_list.append(row_copy)





df = pd.read_csv(sys.stdin)
df = v_today1_v_date_of_birth1_v_date_of_death(df)
df['PatientDeceased'] = df.apply(D_patientDeceased, axis=1)

# Appliquer la validation non vide sur la colonne 'Hospital'
df = df[df.apply(V_NotNull1, axis=1)]




warnings_list = []
df.apply(lambda row: V_length50(row, warnings_list), axis=1)


df.apply(lambda row: V_length100(df, row, warnings_list), axis=1)

#FileDateCreation
file_date_creation = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
df['FileDateCreation'].iloc[0:1] = file_date_creation


# Lire le fichier CSV existant et ajouter les lignes avec des avertissements
duplicates_file_path = '/home/bachir/Bureau/S8/HAI823I TER/resultats/duplicates_file.csv'
duplicates_df = pd.read_csv(duplicates_file_path)
warnings_df = pd.DataFrame(warnings_list)
duplicates_df = duplicates_df.append(warnings_df, ignore_index=True)

# Écrire le DataFrame modifié dans le fichier CSV
duplicates_df.to_csv(duplicates_file_path, index=False)

# Supprimer les colonnes qui contiennent uniquement des valeurs NULL pour toutes les lignes
enlever_all_null_colonnes(df)

df.to_csv(sys.stdout, index=False)
