from operator import truediv
import sys
import os
import pandas as pd
import re
import datetime
from datetime import datetime, timedelta
import numpy as np
import json


def get_rules_from_nifi_properties(file_path):
    with open(file_path, 'r') as file:
        for line in file:
            if line.startswith('columns_and_rules='):
                json_str = line.strip().split('=')[1]
                return json.loads(json_str)
    return None

def V_today1(df, column_name, reject_list):
    today = datetime.now()

    def check_date(row):
        dob = pd.to_datetime(row[column_name], errors='coerce')
        if dob > today:
            row_copy = row.copy()
            row_copy['Rejet'] = 'V-Today-1'
            reject_list.append(row_copy)
            return False
        return True
    df = df[df.apply(check_date, axis=1)]
    return df


def V_date_of_birth1(df, column_name,reject_list):
    today = datetime.now()
    max_dob = today - timedelta(days=125 * 365)
    #TODO : les lignes rejetées on les écrit dans le fichier de rapport
    def check_dates(row):
        dob = pd.to_datetime(row[column_name], errors='coerce')
        
        if dob is not pd.NaT and dob < max_dob:
            row_copy = row.copy()
            row_copy['Rejet'] = 'V-DateOfBirth-1'
            reject_list.append(row_copy)
            return False

        return True

    df = df[df.apply(check_dates, axis=1)]

    return df

def V_dateOfDeath(df, column_name,reject_list):

    def check_dates(row):
        dob = pd.to_datetime(row['DateOfBirth'], errors='coerce')
        dod = pd.to_datetime(row[column_name], errors='coerce')
        
        if dod is not pd.NaT and dob is not pd.NaT and dod < dob:
            row_copy = row.copy()
            row_copy['Rejet'] = 'V-DateofDeath'
            reject_list.append(row_copy)
            return False

        return True

    df = df[df.apply(check_dates, axis=1)]

    return df

def D_patientDeceased(row,column_name):
    if pd.isna(row['PatientDeceased']):
        if pd.notna(row[column_name]):
            return 'Oui'
    return row['PatientDeceased']


def V_NotNull1(row,column_name, reject_list):
    if pd.isna(row[column_name]) or row[column_name] == '':
        row_copy = row.copy()
        row_copy['Rejet'] = 'V-NotNull-1'
        reject_list.append(row_copy)
        return False
    return True

def V_length50(row, column_name, warnings_list):
    if len(str(row[column_name])) > 50:
        row_copy = row.copy()
        row_copy['Avertissement'] = 'V-length50'
        warnings_list.append(row_copy)


def V_length100(row, column_name, warnings_list):
    if len(str(row[column_name])) > 100:
        row_copy = row.copy()
        row_copy['Avertissement'] = 'V-length100'
        warnings_list.append(row_copy)

def V_alpha2(row, column_name,warnings_list):
    if not str(row[column_name]).isalpha():
        row_copy = row.copy()
        row_copy['Avertissement'] = 'V_alpha2'
        warnings_list.append(row_copy)


def enlever_all_null_colonnes(df):
    for col in df.columns:
        if df[col].isna().all():
            df.drop(col, axis=1, inplace=True)

def main():

    nifi_properties_path = "/home/bachir/Téléchargements/nifi-1.20.0-bin/nifi-1.20.0/conf/nifi.properties"
    rules={}
    rules = get_rules_from_nifi_properties(nifi_properties_path)
    df = pd.read_csv(sys.stdin)
    warnings_list = []
    reject_list = []
    validation_functions = {
        "V_today1": V_today1,
        "V_dateOfBirth1": V_date_of_birth1,
        "V_dateOfDeath": V_dateOfDeath,
        "D_patientDeceased": D_patientDeceased,
        "V_NotNull1": V_NotNull1,
        "V_length50": V_length50,
        "V_length100": V_length100,
        "V_alpha2": V_alpha2,
    }

    for column, functions in rules.items():
        for function_name in functions:
            if function_name in validation_functions:
                function = validation_functions[function_name]
                if function_name == "D_patientDeceased":
                    df['PatientDeceased'] = df.apply(lambda row: function(row, column), axis=1)
                elif function_name in ["V_length50", "V_length100", "V_alpha2"]:
                    df.apply(lambda row: function(row, column, warnings_list), axis=1)
                elif function_name == "V_NotNull1" :
                    df = df[df.apply(lambda row: function(row, column,reject_list), axis=1)]
                else:
                    df = function(df, column,reject_list)

    #FileDateCreation
    file_date_creation = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    df['FileDateCreation'].iloc[0:1] = file_date_creation


    # Lire le fichier CSV existant et ajouter les lignes avec des avertissements
    duplicates_file_path = '/home/bachir/Bureau/S8/HAI823I TER/resultats/duplicates_file.csv'
    duplicates_df = pd.read_csv(duplicates_file_path)
    warnings_df = pd.DataFrame(warnings_list)
    rejections_df = pd.DataFrame(reject_list)
    duplicates_df = duplicates_df.append(warnings_df, ignore_index=True)
    duplicates_df = duplicates_df.append(rejections_df, ignore_index=True)

    # Écrire le DataFrame modifié dans le fichier CSV
    duplicates_df.to_csv(duplicates_file_path, index=False)

    # Supprimer les colonnes qui contiennent uniquement des valeurs NULL pour toutes les lignes
    enlever_all_null_colonnes(df)




    df.to_csv(sys.stdout, index=False)

if __name__ == "__main__":
    main()
