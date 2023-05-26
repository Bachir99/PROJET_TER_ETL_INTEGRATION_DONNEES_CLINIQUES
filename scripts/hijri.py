import sys
import csv
from convertdate import islamic, gregorian
import re

def hijri_to_gregorian_converter(value):
    if value != '':  # Vérifier si la valeur de la date n'est pas vide
        date_value = value.split(' ')[0]  # Obtenir la partie de la date de la valeur (en ignorant la partie de l'heure)
        hijri_parts = re.split(r'[/-]', date_value)
        if len(hijri_parts) >= 3:  # Vérifier si la liste hijri_parts a au moins trois éléments
            if len(hijri_parts[0]) == 4 and int(hijri_parts[0]) < 1500:
                hijri_day = int(hijri_parts[2])
                hijri_month = int(hijri_parts[1])
                hijri_year = int(hijri_parts[0])
                gregorian_date = islamic.to_gregorian(hijri_year, hijri_month, hijri_day)
                formatted_gregorian_date = f"{gregorian_date[0]:04d}-{gregorian_date[1]:02d}-{gregorian_date[2]:02d} 00:00:00"
                return formatted_gregorian_date
            elif len(hijri_parts[2]) == 4 and int(hijri_parts[2]) < 1500:
                hijri_day = int(hijri_parts[0])
                hijri_month = int(hijri_parts[1])
                hijri_year = int(hijri_parts[2])
                gregorian_date = islamic.to_gregorian(hijri_year, hijri_month, hijri_day)
                formatted_gregorian_date = f"{gregorian_date[0]:04d}-{gregorian_date[1]:02d}-{gregorian_date[2]:02d} 00:00:00"
                return formatted_gregorian_date
    return value  # Retourner la valeur d'origine si la date est nulle ou la conversion n'est pas applicable

if __name__ == '__main__':
    csv_reader = csv.reader(sys.stdin)
    csv_writer = csv.writer(sys.stdout)

    header = next(csv_reader)
    csv_writer.writerow(header)  # Write the header row only once
    columns= [
            'BIRTHDATE',
            'TIME_ARRIVED',
            'TIME_COMPLETE',
            'DATEOFBIRTH',
            'DATEOFDEATH',
            'DIAGNOSISDATETIME',
            'PROCEDUREDATETIME',
            'STARTDATETIME',
            'ENDDATETIME',
            'CANCELLATIONDATE',
            'PRETRIAGETIME',
            'TRIAGESTARTTIME',
            'TRIAGEENDTIME',
            'ORDERDATETIME',
            'RADIOLOGISTREPORTDATETIME',
            'RADIOLOGISTFINALISATIONDATE',
            'COLLECTIONTIME',
            'SAMPLERECEIVEDTIME',
            'SIGNATUREDATETIME',
            'STARTDISPENSETIME',
            'PRESCRIPTIONVALIDATIONTIME',
            'PREOPSTART',
            'PREOPEND',
            'ANAETHESIASTART',
            'ANAETHESIAEND',
            'RECOVERYSTART',
            'RECOVERYEND',
            'PLANNEDSURGERYDATE',
            'ADMIT_DATE',
            'CLINICAL_DISCHARGE_DATE',	
            'PHYSICAL_DISCHARGE_DATE',
            'LINE_ORDER_DATE',
            'DATE_RECORDED',
            'START_DATE',
            'DATE_REGISTERED',
            'ENCOUNTER_START',
            'ENCOUNTER_END',
            'OPERATION_STARTED',
            'OPERATION_END',
            'END_DATE',
            'StartDateTime',
            'EndDateTime',
            'LastUpdateDateTime',
            'RadiologistReportDateTime',
            'RadiologistFinalisationDate',
            'CollectionTime',
            'SampleReceivedTime',
            'SignatureDateTime',
            'StartDispenseTime',
            'PrescriptionValidationTime',
            'PreOpStart',
            'PreOpEnd',
            'AnaethesiaStart',
            'AnaethesiaEnd',
            'RecoveryStart',
            'RecoveryEnd',
            'CancellationDate',
            'ProcedureDateTime',
            'DiagnosisDateTime',
            'TriageStartTime',
            'TriageEndTime',
            'STARTDATETIME',
            'LASTUPDATEDATETIME',
    ]
    


    for row in csv_reader:
        converted_row = []
        for i, value in enumerate(row):
            if i < len(header) and header[i] in columns:
                converted_row.append(hijri_to_gregorian_converter(value))
            else:
                converted_row.append(value)
        csv_writer.writerow(converted_row)