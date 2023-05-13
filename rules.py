from operator import truediv
import sys
import pandas as pd
import datetime
from datetime import datetime, timedelta
import numpy as np
import json
from excel import create_excel

def get_rules_from_nifi_properties(file_path):
    with open(file_path, 'r') as file:
        for line in file:
            if line.startswith('columns_and_rules='):
                json_str = line.strip().split('=')[1]
                return json.loads(json_str)
    return None

def V_today1(df, column_name, reject_list,rejections_count):
    today = datetime.now()
    rule_name = 'V-Today-1'
    def check_date(row):
        dob = pd.to_datetime(row[column_name], errors='coerce')
        if dob > today:
            row_copy = row.copy()
            row_copy['Rejet'] = rule_name
            reject_list.append(row_copy)
            rejections_count[rule_name] += 1
            return False
        return True
    df = df[df.apply(check_date, axis=1)]
    return df


def V_date_of_birth1(df, column_name,reject_list,rejections_count):
    today = datetime.now()
    max_dob = today - timedelta(days=125 * 365)
    rule_name = 'V-DateOfBirth-1'
    def check_dates(row):
        dob = pd.to_datetime(row[column_name], errors='coerce')
        
        if dob is not pd.NaT and dob < max_dob:
            row_copy = row.copy()
            row_copy['Rejet'] = rule_name
            reject_list.append(row_copy)
            rejections_count[rule_name] += 1
            return False

        return True

    df = df[df.apply(check_dates, axis=1)]

    return df

def V_dateOfDeath(df, column_name,reject_list,rejections_count):
    rule_name = 'V-DateofDeath'
    def check_dates(row):
        dob = pd.to_datetime(row['DateOfBirth'], errors='coerce')
        dod = pd.to_datetime(row[column_name], errors='coerce')
        
        if dod is not pd.NaT and dob is not pd.NaT and dod < dob:
            row_copy = row.copy()
            row_copy['Rejet'] = rule_name
            reject_list.append(row_copy)
            rejections_count[rule_name] += 1
            return False

        return True

    df = df[df.apply(check_dates, axis=1)]

    return df

def D_patientDeceased(row,column_name):
    if pd.isna(row['PatientDeceased']):
        if pd.notna(row[column_name]):
            return 'Oui'
    return row['PatientDeceased']


def T_RemoveLeadingZero_1(row, column_name):
    value = row[column_name]
    if isinstance(value, str) and value.startswith('0'):
        row[column_name] = value.lstrip('0')
    return row[column_name]

def V_NotNull1(row,column_name, reject_list,rejections_count):
    rule_name = 'V-NotNull-1'
    if pd.isna(row[column_name]) or row[column_name] == '':
        row_copy = row.copy()
        row_copy['Rejet'] = rule_name
        reject_list.append(row_copy)
        rejections_count[rule_name] += 1
        return False
    return True

def V_NotNull2(row,column_name, warnings_list, warnings_count):
    rule_name = 'V-NotNull-2'
    if pd.isna(row[column_name]) or row[column_name] == '':
        row_copy = row.copy()
        row_copy['Avertissement'] = rule_name
        warnings_list.append(row_copy)
        warnings_count[rule_name] += 1

def V_length50(row, column_name, warnings_list, warnings_count):
    rule_name = 'V-length50'
    if len(str(row[column_name])) > 50:
        row_copy = row.copy()
        row_copy['Avertissement'] = rule_name
        warnings_list.append(row_copy)
        warnings_count[rule_name] += 1

def V_length100(row, column_name, warnings_list, warnings_count):
    rule_name = 'V-length100'
    if len(str(row[column_name])) > 100:
        row_copy = row.copy()
        row_copy['Avertissement'] = rule_name
        warnings_list.append(row_copy)
        warnings_count[rule_name] += 1

def V_alpha1(row, column_name,reject_list, rejections_count):
    rule_name = 'V-alpha-1'
    if not str(row[column_name]).isalpha():
        row_copy = row.copy()
        row_copy['Rejet'] = rule_name
        reject_list.append(row_copy)
        rejections_count[rule_name] += 1
        return False
    return True

def V_alpha2(row, column_name,warnings_list, warnings_count):
    rule_name = 'V-alpha-2'
    if not str(row[column_name]).isalpha():
        row_copy = row.copy()
        row_copy['Avertissement'] = rule_name
        warnings_list.append(row_copy)
        warnings_count[rule_name] += 1

def D_Null_1(row, column_name):
    value = row[column_name]
    if value == '':
        row[column_name] = 'Null'
    return row[column_name]

def enlever_all_null_colonnes(df):
    for col in df.columns:
        if df[col].isna().all():
            df.drop(col, axis=1, inplace=True)


def deduplicate(input_csv,summary_df,rejections_count):
    rule_name = 'Deduplication'
    # Replace 'PatientNumber' with the name of the column you want to use for deduplication
    deduplicated_df = input_csv.drop_duplicates(subset='PatientNumber', keep='first')


    # Find the duplicate rows
    duplicates_df = input_csv[input_csv.duplicated(subset='PatientNumber', keep='first')]

    # Add a column to the duplicates DataFrame indicating that these rows are duplicates
    duplicates_df = duplicates_df.assign(Rejet='Duplication')


    # Compter le nombre de lignes dupliquées
    nb_lignes_dupliquees = len(input_csv) - len(deduplicated_df)
    rejections_count[rule_name] = nb_lignes_dupliquees
    summary_df = summary_df.append({'Rule': 'Duplication', 'Rejected': nb_lignes_dupliquees, 'Warned': 0}, ignore_index=True)

    return deduplicated_df,summary_df,duplicates_df



def main():

    nifi_properties_path = "/home/bachir/Téléchargements/nifi-1.20.0-bin/nifi-1.20.0/conf/nifi.properties"
    rules={}
    rules = get_rules_from_nifi_properties(nifi_properties_path)
    df = pd.read_csv(sys.stdin)
    initial_row_count = len(df)
    
    summary_df = pd.DataFrame(columns=['Rule', 'Rejected', 'Warned','Initial'])
    summary_df = summary_df.append({'Rule': 'Nbr de lignes initial','Initial' : initial_row_count}, ignore_index=True)
    
    warnings_count = {"V-length50":0,
                      "V-length100":0,
                      "V-alpha-2":0,
                      "V-NotNull-2":0}
    rejections_count = {"V-NotNull-1":0,
                        "V-alpha-1":0,
                        "V-Today-1":0,
                        "V-DateOfBirth-1":0,
                        "V-DateofDeath":0,
                        "Deduplication":0
                        }
    df, summary_df, duplicates_df = deduplicate(df,summary_df,rejections_count)
    
    warnings_list = []
    reject_list = []




    validation_functions = {
        "V_today1": V_today1,
        "V_dateOfBirth1": V_date_of_birth1,
        "V_dateOfDeath": V_dateOfDeath,
        "D_patientDeceased": D_patientDeceased,
        "V_NotNull1": V_NotNull1,
        "V_NotNull2": V_NotNull2,
        "V_length50": V_length50,
        "V_length100": V_length100,
        "V_alpha1": V_alpha1,
        "V_alpha2": V_alpha2,
        "T_RemoveLeadingZero_1": T_RemoveLeadingZero_1,
        "D_Null_1": D_Null_1
    }

    for column, functions in rules.items():
        for function_name in functions:
            if function_name in validation_functions:
                function = validation_functions[function_name]
                if function_name == "D_patientDeceased":
                    df['PatientDeceased'] = df.apply(lambda row: function(row, column), axis=1)
                elif function_name in ["T_RemoveLeadingZero_1","D_Null_1"]:
                    df[column] = df.apply(lambda row: function(row, column), axis=1)
                elif function_name in ["V_length50", "V_length100", "V_alpha2","V_NotNull2"]:
                    df.apply(lambda row: function(row, column, warnings_list, warnings_count), axis=1)
                elif function_name in ["V_NotNull1","V_alpha1"] :
                    df = df[df.apply(lambda row: function(row, column,reject_list, rejections_count), axis=1)]
                else:
                    df = function(df, column,reject_list, rejections_count)

    #FileDateCreation
    file_date_creation = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    df['FileDateCreation'].iloc[0:1] = file_date_creation


    for rule, count in rejections_count.items():
        summary_df = summary_df.append({'Rule': rule, 'Rejected': count, 'Warned': 0}, ignore_index=True)

    for rule, count in warnings_count.items():
        if rule in summary_df['Rule'].values:
            summary_df.loc[summary_df['Rule'] == rule, 'Warned'] = count
        else:
            summary_df = summary_df.append({'Rule': rule, 'Rejected': 0, 'Warned': count}, ignore_index=True)

    summary_df.to_csv('/home/bachir/Bureau/S8/HAI823I TER/resultats/report_rules.csv', index=False)

    # Ajouter les lignes avec des avertissements et des rejets au dataframe lines_df pour pouvoir les écrire dans le validation report après
    lines_df = pd.DataFrame()
    warnings_df = pd.DataFrame(warnings_list)
    rejections_df = pd.DataFrame(reject_list)
    lines_df = lines_df.append(duplicates_df,ignore_index=True)
    lines_df = lines_df.append(warnings_df, ignore_index=True)
    lines_df = lines_df.append(rejections_df, ignore_index=True)


    create_excel(lines_df,initial_row_count,warnings_count,rejections_count)


    # Supprimer les colonnes qui contiennent uniquement des valeurs NULL pour toutes les lignes
    enlever_all_null_colonnes(df)




    df.to_csv(sys.stdout, index=False)

if __name__ == "__main__":
    main()
