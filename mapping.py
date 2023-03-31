#!/usr/bin/python3
import pandas as pd
import sys

file = pd.read_csv(sys.stdin)
file = file.rename(columns={'PATIENTID': 'PatientNumber',
                            'BIRTHDATE': 'DateOfBirth',
                            'GENDER': 'Gender',
                            'NATIONALITY': 'Nationality',
                            'MARITALSTATUS': 'Title'})
column_order = ['PatientNumber', 'Gender', 'Nationality', 'DateOfBirth', 'Title']
#df = file.explode(sys.argv[1])
file.to_csv(sys.stdout, index=False)