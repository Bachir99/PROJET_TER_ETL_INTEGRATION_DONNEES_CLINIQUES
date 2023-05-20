from operator import truediv
import sys
import pandas as pd
import datetime
from datetime import datetime, timedelta
import numpy as np
import json
from excel import create_excel
import math

"""def get_rules_from_nifi_properties(file_path):
    with open(file_path, 'r') as file:
        for line in file:
            if line.startswith('columns_and_rules='):
                json_str = line.strip().split('=')[1]
                return json.loads(json_str)
    return None"""

def V_today1(df, column_name, reject_list,rejections_count):
    today = datetime.now()
    rule_name = 'V-Today-1'
    def check_date(row):
        dob = pd.to_datetime(row[column_name], errors='coerce')
        if dob > today:
            row_copy = row.copy()
            row_copy['Rejet'] = rule_name
            reject_list.concat(row_copy)
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
        warnings_list.concat(row_copy)
        warnings_count[rule_name] += 1

def V_length100(row, column_name, warnings_list, warnings_count):
    rule_name = 'V-length100'
    if len(str(row[column_name])) > 100:
        row_copy = row.copy()
        row_copy['Avertissement'] = rule_name
        warnings_list.concat(row_copy)
        warnings_count[rule_name] += 1

def V_alpha1(row, column_name,reject_list, rejections_count):
    rule_name = 'V-alpha-1'
    if not str(row[column_name]).isalpha():
        row_copy = row.copy()
        row_copy['Rejet'] = rule_name
        reject_list.concat(row_copy)
        rejections_count[rule_name] += 1
        return False
    return True

def V_alpha2(row, column_name,warnings_list, warnings_count):
    rule_name = 'V-alpha-2'
    if not str(row[column_name]).isalpha():
        row_copy = row.copy()
        row_copy['Avertissement'] = rule_name
        warnings_list.concat(row_copy)
        warnings_count[rule_name] += 1
#################""

def T_EncounterNumber_1(row,column_name):
    return f"{row['Hospital']}-{row['EncounterType']}"

def T_BedNumber_1(row, column_name):
    return f"{row['Hospital']}-{row['Ward']}-{row['RoomNumber']}"

def T_PatientNumber_1(row,column_name):
        return f"{row['Hospital']}-{row['SourcePatientNumber']}-{row['RoomNumber']}"
        #TODO : supprimer la colonne Hospital
        
def D_BedNumber_1(row, column_name, warnings_list, warnings_count):
    
    rule_name = 'D-BedNumber-1'
    bed_number = row[column_name] if pd.notnull(row[column_name]) else 'NULL'
    
    if pd.isnull(row[column_name]):
        row_copy = row.copy()
        row_copy['Avertissement'] = rule_name
        warnings_list.append(row_copy)
        warnings_count[rule_name] += 1
    
    return f"{row['Hospital']}-{row['Ward']}-{row['RoomNumber']}-{bed_number}"

def D_RoomNumber_1(row, column_name, warnings_list, warnings_count):
    rule_name = 'D-RoomNumber-1'
    room_number = row[column_name] if pd.notnull(row[column_name]) else 'NULL'  
    if pd.isnull(row[column_name]):
        row_copy = row.copy()
        row_copy['Avertissement'] = rule_name
        warnings_list.append(row_copy)
        warnings_count[rule_name] += 1
    return f"{row['Hospital']}-{row['Ward']}-{room_number}"

def T_RoomNumber_1(row, room_number_column):
    room_number_combined = f"{row['Hospital']}-{row['Ward']}-{row[room_number_column]}"
    row[room_number_column] = room_number_combined
    return row

def V_Num_1(row, column_name, reject_list, rejections_count):
    rule_name = 'V-Num-1'
    if not str(row[column_name]).isnumeric():
        row_copy = row.copy()
        row_copy['Rejet'] = rule_name
        reject_list.append(row_copy)
        rejections_count[rule_name] += 1

def V_Quantity_1(row, column_name, reject_list, rejections_count):
    rule_name = 'V-Quantity-1'
    if not row[column_name] > 0:
        row_copy = row.copy()
        row_copy['Rejet'] = rule_name
        reject_list.append(row_copy)
        rejections_count[rule_name] += 1


def D_Age_1(row,column_name_age, warnings_list, warnings_count):
    rule_name = 'D-Age-1'
    
    if pd.notnull(row['date_of_birth']):
        age_years = row['start_date'].year - row['date_of_birth'].year
        if row['start_date'].month < row['date_of_birth'].month or (row['start_date'].month == row['date_of_birth'].month and row['start_date'].day < row['date_of_birth'].day):
            age_years -= 1
        
        if age_years != row[column_name_age]:
            row_copy = row.copy()
            row_copy['Avertissement'] = rule_name
            warnings_list.append(row_copy)
            warnings_count[rule_name] += 1
        
        return age_years
    else:
        return row[column_name_age]  # Conserve la valeur originale si DateOfBirth est NULL

def D_Null_1(row, column_name):
    value = row[column_name]
    if value == '':
        row[column_name] = 'Null'
    return row[column_name]

def T_RoundNum92_1(row, column_name):
    return round(row[column_name], 2)

def V_GTE0_1(row, column_name, reject_list, rejections_count):
    rule_name = 'V-GTE0-1'
    if not row[column_name] >= 0:
        row_copy = row.copy()
        row_copy['Rejet'] = rule_name
        reject_list.append(row_copy)
        rejections_count[rule_name] += 1

def D_Duration_1(row,column_name_duration, warnings_list, warnings_count):
    rule_name = 'D-Duration-1'
    if pd.notnull(row['EndDateTime']):
        duration = (row['EndDateTime'] - row['StartDateTime']).total_seconds() / 60.0
        duration = round(duration) # convertir en nombre entier
        if duration != row[column_name_duration]:
            row_copy = row.copy()
            row_copy['Avertissement'] = rule_name
            warnings_list.append(row_copy)
            warnings_count[rule_name] += 1
        return duration
    else:
        return row[column_name_duration] # conserver la valeur originale si 'EndDateTime' est NULL

def T_RoundInteger_1(row, column_name):
    value = row[column_name]
    return math.ceil(value)

def D_DummyEncounterNumber_1(row,column_name):
    encounter_number = f"{row['Hospital']}-{row['PatientNumber']}-{row['ServicingDepartment']}-{row['StartDate'].strftime('%d%m%Y')}"
    return encounter_number

def enlever_all_null_colonnes(df):
    for col in df.columns:
        if df[col].isna().all():
            df.drop(col, axis=1, inplace=True)


def deduplicate(input_csv,rejections_count):
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

    return deduplicated_df,duplicates_df



def main():

    #TODO : tout ce qui est date on leur applique toutes les règles des dates malgré que c'est pas mentionné, psq les dates c'est considéré comme mapping !
    #TODO : Après avoir appliqué T-PatientNumber-1 faut supprimer la colonne Hospital
    #TODO : Transfer, Diagnosis, Procedure dans EncounterNumber le EncounterType dans T-EncounterNumber-1 est toujours IP
    #TODO : Après avoir appliqué EncounterNumber dans SERVICE faut supprimer la colonne EncounterType
    
    rules={"DateOfBirth": ["V_today1", "V_dateOfBirth1",], 
           "DateofDeath": ["V_today1", "V_dateOfBirth1","V_dateOfDeath", "D_patientDeceased"], 
           "Hospital": ["V_NotNull1"],
           "PatientNumber": ["V_length50"],
           "FathersName": ["V_length100", "V_alpha2"], 
           "FathersPreName": ["V_length100", "V_alpha2"],
           "PlaceOfBirth": ["V_length100"],
           "SourcePatientNumber" :  ["V_length50", "T_PatientNumber_1"],
           "PatientDeceased" : ["D_patientDeceased"],
           "DateOfDeath" :  ["V_dateOfDeath", "V_dateOfBirth1","V_today1"],
           "Title" :  ["V_alpha2", "V_dateOfBirth1","V_today1"],
           "EncounterNumber" :  ["V_NotNull1", "T_EncounterNumber_1"],
           "BedNumber" :  ["T_BedNumber_1", "D_BedNumber_1"],
           "Age" :  ["D_Age_1"],
           "RoomNumber" : ["T_RoomNumber_1", "D_RoomNumber_1"],
           "DiagnosisCode" : ["V_NotNull1"],
           "DiagnosisVersion" : ["V_NotNull1"],
           "Sequence" : ["V_NotNull1","V_Num_1", "T_RoundInteger_1"],
           "ProcedureVersion" :  ["V_NotNull1"],
           "Quantity" : ["V_NotNull1", "V_Quantity_1" , "T_RoundNum92_1"],
           "Duration" : ["V_GTE0_1","D_Duration_1",  "T_RoundNum92_1"],
           "ServiceDescription" : ["V_length100"]}

    df = pd.read_csv(sys.stdin)
    initial_row_count = len(df)
    
    summary_df = pd.DataFrame(columns=['Rule', 'Rejected', 'Warned','Initial'])
    initial_df = pd.DataFrame({'Rule': 'Nbr de lignes initial','Initial' : initial_row_count}, index=[0])
    summary_df = pd.concat([summary_df, initial_df], ignore_index=True)

    
    warnings_count = {"V-length50":0,
                      "V-length100":0,
                      "V-alpha-2":0,
                      "V-NotNull-2":0,
                      "D-BedNumber-1":0,
                      "D-RoomNumber-1":0,
                      "D-Age-1" :0,
                      "D-Duration-1" :0

                      }
    rejections_count = {"V-NotNull-1":0,
                        "V-alpha-1":0,
                        "V-Today-1":0,
                        "V-DateOfBirth-1":0,
                        "V-DateofDeath":0,
                        "Deduplication":0,
                        "V-Quantity-1":0,
                        "V-GTE0-1":0,
                        "V-Num-1":0
                        }
    df, duplicates_df = deduplicate(df,rejections_count)
    
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
        "D_Null_1": D_Null_1,
        "T_BedNumber_1": T_BedNumber_1,
        "T_PatientNumber_1 ": T_PatientNumber_1,
        "T_EncounterNumber_1" : T_EncounterNumber_1,
        "D_BedNumber_1" : D_BedNumber_1,
        "T_RoomNumber_1" : T_RoomNumber_1,
        "V_Num_1" : V_Num_1,
        "D_Age_1" : D_Age_1,
        "D_Null_1" : D_Null_1,
        "T_RoundNum92_1" : T_RoundNum92_1,
        "V_GTE0_1" : V_GTE0_1,
        "D_Duration_1" : D_Duration_1,
        "D_RoomNumber_1" : D_RoomNumber_1,
        "V_Quantity_1" : V_Quantity_1,
        "T_RoundInteger_1" : T_RoundInteger_1,
        "D_DummyEncounterNumber_1" : D_DummyEncounterNumber_1
    }
    for column, functions in rules.items():
        for function_name in functions:
            if function_name in validation_functions:
                function = validation_functions[function_name]
                if function_name == "D_patientDeceased":
                    df['PatientDeceased'] = df.apply(lambda row: function(row, column), axis=1)
                elif function_name in ["T_RemoveLeadingZero_1","D_Null_1", "T_BedNumber_1","T_EncounterNumber_1", "T_RoomNumber_1","T_RoundNum92_1","T_RoundInteger_1","D_DummyEncounterNumber_1"]:  # Ajoutez ici le nom de votre règle
                    df[column] = df.apply(lambda row: function(row, column), axis=1)
                elif function_name == "T_PatientNumber_1":
                    df[column] = df.apply(lambda row: function(row, column), axis=1)
                    # Remove the 'Hospital' column
                    df.drop('Hospital', axis=1, inplace=True)
                elif function_name in ["V_length50", "V_length100", "V_alpha2","V_NotNull2","D_BedNumber_1", "D_RoomNumber_1","D_Age_1","D_Duration_1"]:
                    df.apply(lambda row: function(row, column, warnings_list, warnings_count), axis=1)
                elif function_name in ["V_NotNull1","V_alpha1","V_Num_1","V_Quantity_1","V_GTE0_1"] :
                    df = df[df.apply(lambda row: function(row, column,reject_list, rejections_count), axis=1)]
                else:
                    df = function(df, column,reject_list, rejections_count)

    #FileDateCreation
    file_date_creation = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    #df.loc[0:0, 'FileDateCreation'] = file_date_creation
    df['FileDateCreation'].iloc[0:1] = file_date_creation


    # Ajouter les lignes avec des avertissements et des rejets au dataframe lines_df pour pouvoir les écrire dans le validation report après
    lines_df = pd.DataFrame()
    warnings_df = pd.DataFrame(warnings_list)
    rejections_df = pd.DataFrame(reject_list)
    lines_df = pd.concat([lines_df, duplicates_df, warnings_df, rejections_df], ignore_index=True)


    create_excel(lines_df,initial_row_count,warnings_count,rejections_count)


    # Supprimer les colonnes qui contiennent uniquement des valeurs NULL pour toutes les lignes
    enlever_all_null_colonnes(df)




    df.to_csv(sys.stdout, index=False)

if __name__ == "__main__":
    main()
