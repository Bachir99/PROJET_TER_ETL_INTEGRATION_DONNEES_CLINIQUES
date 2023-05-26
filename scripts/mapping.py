import csv
import pandas as pd
import sys
import re
from collections import OrderedDict
from excel import create_excel
import os
# Dictionnaire key:value qui contient les correspondances de noms de colonnes

def recuperate(file_path):
    with open(file_path, 'r') as file:
        content = file.read().strip()
    return content

def mapping(df, column_mapping, nom_fichier, type_fichier,warnings_count, rejections_count):
    
    #Les mettre en majuscule
    column_mapping = {key.upper(): value for key, value in column_mapping.items()}
    
    
    if len(column_mapping)==0:
        create_excel(df,len(df),warnings_count,rejections_count)
        df = pd.DataFrame()
        df.to_csv(sys.stdout, sep=',', index=False)
    else :
        # Réorganiser les colonnes du DataFrame en suivant l'ordre défini dans le dictionnaire
        df = df[column_mapping.values()]
        
        if type_fichier == 'Patient' : #Un fichier Patient
            # Itération sur chaque colonne du dataframe
            for new_names, original_name in column_mapping.items():
                # Vérifier si la clé (le nom de colonne) est présente dans le dictionnaire
                if original_name in df.columns:
                    # Renommage de la colonne
                    df.rename(columns={original_name: new_names}, inplace=True)
            
            # Recherche du nom de l'hôpital dans le nom du fichier
            hopital_name = re.search(r'_(.*?)_', nom_fichier)
           
            if hopital_name is not None:
                df.insert(1, 'HOSPITAL', hopital_name.group(1))

            # Écriture du fichier CSV résultant sur la sortie standard (stdout)
            df.to_csv(sys.stdout, sep=',', index=False)
        
        elif type_fichier == 'Encounter' : #Un fichier Encounter
            # Itération sur chaque colonne du dataframe
            for new_names, original_name in column_mapping.items():
                # Vérifier si la clé (le nom de colonne) est présente dans le dictionnaire
                if original_name in df.columns:
                    # Renommage de la colonne
                    df.rename(columns={original_name: new_names}, inplace=True)
            
            # Recherche du nom de l'hôpital dans le nom du fichier
            encounterType = re.search('OP|IP|ED', nom_fichier)
            
            if encounterType:
                # Récupération de la valeur correspondante
                encounterType_value = encounterType.group()
                # Création de la colonne "ENCOUNTERTYPE" avec le nom de l'hôpital
                df.insert(6, 'ENCOUNTERTYPE', encounterType_value)
                        # Recherche du nom de l'hôpital dans le nom du fichier

            # Écriture du fichier CSV résultant sur la sortie standard (stdout)
            df.to_csv(sys.stdout, sep=',', index=False)            
                
        elif type_fichier == 'Transfer' : #un fichier TRANSFER
            
            # Itération sur chaque colonne du dataframe
            for new_names, original_name in column_mapping.items():
                # Vérifier si la clé (le nom de colonne) est présente dans le dictionnaire
                if original_name in df.columns:
                    # Renommage de la colonne
                    df.rename(columns={original_name: new_names}, inplace=True)
            
            # Recherche du nom de l'hôpital dans le nom du fichier
            hopital_name = re.search(r'_(.*?)_', nom_fichier)
            if hopital_name is not None:
                df.insert(1, 'HOSPITAL', hopital_name.group(1))

            #TODO : BedNumber et RoomNumber c'est des colonnes qu'on doit en déduire
            
            
            # Écriture du fichier CSV résultant sur la sortie standard (stdout)
            df.to_csv(sys.stdout, sep=',', index=False)            
        
        elif type_fichier == 'Diagnosis' : #un fichier DIAGNOSIS
            
            # Itération sur chaque colonne du dataframe
            for new_names, original_name in column_mapping.items():
                # Vérifier si la clé (le nom de colonne) est présente dans le dictionnaire
                if original_name in df.columns:
                    # Renommage de la colonne
                    df.rename(columns={original_name: new_names}, inplace=True)
            
            # Recherche du nom de l'hôpital dans le nom du fichier
            hopital_name = re.search(r'_(.*?)_', nom_fichier)
            if hopital_name is not None:
                df.insert(1, 'HOSPITAL', hopital_name.group(1))

            # Écriture du fichier CSV résultant sur la sortie standard (stdout)
            df.to_csv(sys.stdout, sep=',', index=False)          
        elif type_fichier == 'Procedure' : #Un fichier procédure
            
            # Itération sur chaque colonne du dataframe
            for new_names, original_name in column_mapping.items():
                # Vérifier si la clé (le nom de colonne) est présente dans le dictionnaire
                if original_name in df.columns:
                    # Renommage de la colonne
                    df.rename(columns={original_name: new_names}, inplace=True)
            
            # Recherche du nom de l'hôpital dans le nom du fichier
            hopital_name = re.search('(Hop.*)\.csv', nom_fichier)
            
            if hopital_name is not None:
                df.insert(1, 'Hospital', hopital_name.group(1))          
            
            # Écriture du fichier CSV résultant sur la sortie standard (stdout)
            df.to_csv(sys.stdout, sep=',', index=False) 

        elif type_fichier == 'Service' :
            
            # Itération sur chaque colonne du dataframe
            for new_names, original_name in column_mapping.items():
                # Vérifier si la clé (le nom de colonne) est présente dans le dictionnaire
                if original_name in df.columns:
                    # Renommage de la colonne
                    df.rename(columns={original_name: new_names}, inplace=True)
            
            # Recherche du nom de l'hôpital dans le nom du fichier
            hopital_name = re.search(r'_(.*?)_', nom_fichier)
           
            if hopital_name is not None:
                df.insert(1, 'HOSPITAL', hopital_name.group(1))         
            
            match = re.search(r'Serv\.([A-Za-z0-9.]+)_', nom_fichier)
            if match:
                servicingDepartment = match.group(1)
            
            # Insérer la nouvelle colonne
            position = df.columns.get_loc('ENCOUNTERNUMBER') + 1
            df.insert(position, 'SERVICINGDEPARTMENT', servicingDepartment)              
            
            # Écriture du fichier CSV résultant sur la sortie standard (stdout)
            df.to_csv(sys.stdout, sep=',', index=False) 
            
            


dict_patient = {
    #Out          #input
'PatientNumber':'MRN Number',		
'DateOfBirth':'DateOfBirth',
'Gender':'Gender',
'Extra:PatientDeceased' : 'PatientDeceased',
'Extra:DateofDeath' : 'DateofDeath',
'Extra:PlaceOfBirth' : 'PlaceOfBirth',	
'EthnicOrigin' : 'EthnicOrigin',
'Extra:Nationality' : 'Nationality',
'LastName' : 'LastName',
'FirstName' : 'FirstName',
'Title' : 'Title',
'Extra:MothersLastName' : 'MothersName',
'Extra:MothersFirstName' : 'MothersPreName',	
'Extra:FathersLastName' : 'FathersName',	
'Extra:FathersFirstName' : 'FathersPreName',
'Extra:FamilyDoctor' : 'FamilyDoctor',	
'Extra:BloodRefusal' : 'BloodRefusal',	
'Extra:OrganDonor' : 'OrganDonor',	
'Extra:PrefLanguage' : 'PrefLanguage',	
'Extra:LastUpdateDateTime' : 'LastUpdateDateTime',
'NationalIdentifier' : 'NationalID'
}

dict_encounter = {
'PatientNumber': 'SourcePatientNumber',
'Hospital': 'Hospital',
'StartDateTime': 'StartDateTime',
'EndDateTime': 'EndDateTime',
'EncounterNumber': 'EncounterNumber',
'Age': 'Age',
#'EncounterType': 'EncounterType',
'EncounterCategory': 'EncounterCategory',
'LengthOfStay': 'LengthOfStay',
'AdmitWard': 'AdmitWard',
'DischargeWard': 'DischargeWard',
'ReferringConsultant': 'ReferringConsultant',
'Extra:ReferringConsultantName': 'ReferringConsultantName',
'ReferringConsultantSpecialty': 'ReferringConsultantSpecialty',
'AdmittingConsultant': 'AdmittingConsultant',
'Extra:AdmittingConsultantName': 'AdmittingConsultantName',
'AdmittingConsultantSpecialty': 'AdmittingConsultantSpecialty',
'AttendingConsultant': 'AttendingConsultant',
'Extra:AttendingConsultantName': 'AttendingConsultantName',
'AttendingConsultantSpecialty': 'AttendingConsultantSpecialty',
'DischargeConsultant': 'DischargeConsultant',
'Extra:DischargeConsultantName': 'DischargeConsultantName',
'DischargeConsultantSpecialty': 'DischargeConsultantSpecialty',
'Extra:TransferToHospital': 'TransferToHospital',
'Extra:CauseOfDeath': 'CauseOfDeath',
'Extra:TypeOfDeath': 'TypeOfDeath',
'Extra:DateofDeath': 'DateofDeath',
'Extra:Autopsy': 'Autopsy',
'DRG1': 'DRG1',
'DRG1Version': 'DRG1Version',
'Extra:DRGGravity': 'DRGGravity',
'Extra:MDC': 'MDC',
'Extra:LastUpdateDateTime': 'LastUpdateDateTime',
'DischargeDestination': 'DischargeDestination',
'Address': 'Address',
'PostCode': 'PostCode',
'Extra:Municipality': 'Municipality',
'Suburb': 'Suburb',
'Extra:Region': 'Region',
'Extra:Country': 'Country',
'Extra:LivingArrangements': 'LivingArrangements',
'MaritalStatus': 'MaritalStatus',
'AdmissionCategory': 'AdmissionCategory',
'AdmissionSource': 'AdmissionSource',
'AdmissionElection': 'AdmissionElection',
'HealthFund': 'HealthFund',
'FinancialClass': 'FinancialClass',
'Extra:TransferFromHospital': 'TransferFromHospital',
'EXTRA:ClinicName': 'ClinicName',
'EXTRA:ClinicSpecialtyCode': 'ClinicSpecialtyCode',
'EXTRA:ClinicSpecialty': 'ClinicSpecialty',
'EXTRA:ModeOfArrival': 'ModeOfArrival',
'EXTRA:PreTriageTime': 'PreTriageTime',
'EXTRA:TriageStartTime': 'TriageStartTime',
'EXTRA:TriageEndTime': 'TriageEndTime',
'EXTRA:DiagnosisOnDischarge': 'DiagnosisOnDischarge',
'EXTRA:PhysicianSpecialityKey': 'PhysicianSpecialityKey',
'EXTRA:CancellationDate': 'CancellationDate',
'EXTRA:CancellationFlag':'CancellationFlag',
'Extra:VisitType':'VisitType',
'Extra:Site':'Site',
'Extra:DischargeStatus':'DischargeStatus',
'Extra:ComplaintDesc':'ComplaintDesc',
'Extra:TriageCode':'TriageCode',
'Extra:TriageDesc':'TriageDesc'
}

dict_transfer = {
'PatientNumber': 'SourcePatientNumber',
'Extra:Hospital': 'Hospital',
'BedNumber': 'BedNumber',
'EncounterNumber': 'EncounterNumber',
'Ward': 'Ward',
'StartDateTime': 'StartDateTime',
'Extra:RoomNumber': 'RoomNumber',
'Extra:WardType': 'WardType',
'Leave': 'Leave',
'Extra:LeaveType': 'LeaveType',
'AttendingConsultant_Code': 'AttendingConsultant_Code',
'Extra:AttendingConsultantName': 'AttendingConsultantName',
'AttendingConsultant_SpecialtyCode': 'AttendingConsultant_SpecialtyCode',
'Extra:LastUpdateDateTime': 'LastUpdateDateTime',
'Extra:Site': 'Site'
}

dict_diagnosis = {
    'Extra:SourcePatientNumber': 'SourcePatientNumber',
    'Extra:Hospital': 'Hospital',
    'EncounterNumber': 'EncounterNumber',
    'DiagnosisCode': 'DiagnosisCode',
    'DiagnosisVersion': 'DiagnosisVersion',
    'Sequence': 'Sequence',
    'Extra:DiagnosisType': 'DiagnosisType',
    'ConditionOnset': 'ConditionOnset',
    'Extra:SequenceService': 'SequenceService',
    'Extra:PrimaryTumour': 'PrimaryTumour',
    'Extra:TumourCode': 'TumourCode',
    'Extra:Metastase': 'Metastase',
    'Extra:Ganglion': 'Ganglion',
    'Extra:StageEvolution': 'StageEvolution',
    'Extra:Morphology': 'Morphology',
    'Extra:Screening': 'Screening',
    'Extra:DiagnosisDateTime': 'DiagnosisDateTime',
    'Extra:CodeCharacteristic': 'CodeCharacteristic',
    'Extra:CodeCharacteristicDesc': 'CodeCharacteristicDesc',
    'Extra:LocalDiagCode': 'LocalDiagCode',
    'DiagnosisDescription': 'LocalDiagCodeDesc',
    'Extra:LastUpdateDateTime': 'LastUpdateDateTime'
}

dict_procedure = {
'Extra:SourcePatientNumber': 'SourcePatientNumber',
'Extra:Hospital': 'Hospital',
'EncounterNumber': 'EncounterNumber',
'ProcedureDateTime': 'ProcedureDateTime',
'ProcedureCode': 'ProcedureCode',
'ProcedureVersion': 'ProcedureVersion',
'Sequence': 'Sequence',
'Extra:InterventionType': 'InterventionType',
'Consultant': 'Consultant',
'Extra:ConsultantName': 'ConsultantName',
'ConsultantSpecialty': 'ConsultantSpecialty',
'ProcedureTheatre': 'ProcedureTheatre',
'Extra:LocalProcTheatre': 'LocalProcTheatre',
'Extra:LocalProcTheatreDesc': 'LocalProcTheatreDesc',
'Extra:NbrProcedures': 'NbrProcedures',
'Extra:LastUpdateDateTime': 'LastUpdateDateTime'
}

dict_service = {
'PatientNumber': 'SourcePatientNumber',
'Hospital': 'Hospital',
'StartDateTime': 'StartDateTime',
'Quantity': 'Quantity',
'ServiceCode': 'ServiceCode',
'Extra:PrimaryProcedure': 'PrimaryProcedure',
'EncounterNumber': 'EncounterNumber',
'ServicingDepartment': 'ServicingDepartment',
'Duration': 'Duration',
'ActualCharge': 'ActualCharge',
'EndDateTime': 'EndDateTime',
'PointOfService1': 'PointOfService1',
'Extra:ServiceDescription': 'ServiceDescription',
'Extra:ServiceGroup': 'ServiceGroup',
'Extra:LastUpdateDateTime': 'LastUpdateDateTime',
'Consultant': 'Consultant',
'Extra:ConsultantName': 'ConsultantName',
'ConsultantSpecialty': 'ConsultantSpecialty',
'Clinic': 'Clinic',
'OrderDateTime': 'OrderDateTime',
'Extra:PriorityCode': 'PriorityCode',
'Extra:Priority': 'Priority',
'Extra:StatusCode': 'StatusCode',
'Extra:StartDateTreatmentPlan': 'StartDateTreatmentPlan',
'Extra:EndDateTreatmentPlan': 'EndDateTreatmentPlan',
'Extra:RequestNo': 'RequestNo',
'Extra:OrderingDepartment': 'OrderingDepartment',
'Extra:PrivateInsurance': 'PrivateInsurance',
'Extra:OriginalServiceCode': 'OriginalServiceCode',
'Extra:OriginalServiceDesc': 'OriginalServiceDesc',
'Extra:OriginalServiceGroup': 'OriginalServiceGroup',
'Extra:RadiographerExamDuration': 'RadiographerExamDuration',
'Extra:RadiologistLicence': 'RadiologistLicence',
'Extra:RadiologistName': 'RadiologistName',
'Extra:RadiologistSpecialty': 'RadiologistSpecialty',
'Extra:RadiologistReportDateTime': 'RadiologistReportDateTime',
'Extra:RadiologistFinalisationDate': 'RadiologistFinalisationDate',
'Extra:RadiologistReportDuration': 'RadiologistReportDuration',
'Extra:StaffSignoff': 'StaffSignoff',
'Extra:CollectionTime': 'CollectionTime',
'Extra:SampleReceivedTime': 'SampleReceivedTime',
'TestResult': 'TestResult',
'Extra:SignatureDateTime': 'SignatureDateTime',
'Extra:PathologistName': 'PathologistName',
'Extra:PathologistLicence': 'PathologistLicence',
'Extra:ServiceGroupDesc': 'ServiceGroupDesc',
'Extra:DIN': 'DIN',
'Extra:StartDispenseTime': 'StartDispenseTime',
'Extra:PrescriptionValidationTime': 'PrescriptionValidationTime',
'Extra:QuantityAdministered': 'QuantityAdministered',
'Extra:QuantityPrescribed': 'QuantityPrescribed',
'Extra:ProcedureSpecialty': 'ProcedureSpecialty',
'Extra:ElectiveOrEmergency': 'ElectiveOrEmergency',
'Extra:PreOpStart': 'PreOpStart',
'Extra:PreOpEnd': 'PreOpEnd',
'Extra:AnaethesiaStart': 'AnaethesiaStart',
'Extra:AnaethesiaEnd': 'AnaethesiaEnd',
'Extra:RecoveryStart': 'RecoveryStart',
'Extra:RecoveryEnd': 'RecoveryEnd',
'Extra:NumberXtraMedicalStaff': 'NumberXtraMedicalStaff',
'Extra:NumberExtraPersons': 'NumberExtraPersons',
'Extra:NumberTheatreNurses': 'NumberTheatreNurses',
'Extra:NumberTheatreNursesAux': 'NumberTheatreNursesAux',
'Extra:OncologyFlag': 'OncologyFlag',
'Extra:PatientType': 'PatientType',
'Extra:CancellationDate': 'CancellationDate',
'Extra:CancellationReasonCode': 'CancellationReasonCode',
'Extra:CancellationReasonDesc': 'CancellationReasonDesc',
'Extra:AnaesthetistCode': 'AnaesthetistCode',
'Extra:AnaesthetistName': 'AnaesthetistName',
'Extra:AnaestheticTechnique': 'AnaestheticTechnique',
'Extra:RequestStatus': 'RequestStatus',
'Extra:PlannedSurgeryDate': 'PlannedSurgeryDate',
'Extra:OperationID': 'OperationID',
'Extra:OperationStatus': 'OperationStatus',
#'EncounterType': 'EncounterType',
'Extra:PACUDuration': 'PACUDuration',
'Extra:Implants': 'Implants',
'Extra:Site': 'Site',
'Extra:TestName': 'TestName',
'Extra:OrderingConsultant': 'OrderingConsultant',
'Extra:OrderingConsultantSpecialty': 'OrderingConsultantSpecialty'
}



df = pd.read_csv(sys.stdin,dtype=str)

#TODO : Replace Hospital with the missing mandatory field, depends on the file

rejections_count = {'Absence MandatoryField':{'Hospital':len(df)}}

warnings_count = {
        "V-length50": {},
        "V-length100": {},
        "V-alpha-2": {},
        "V-NotNull-2": {},
        "D-BedNumber-1": {},
        "D-RoomNumber-1": {},
        "D-Age-1": {},
        "D-Duration-1": {}
    }


    # Initialisation des dictionnaires de dictionnaires
for rule_name in warnings_count:
    warnings_count[rule_name] = {'A': 0}



file_name_path = "./file_name.txt"
# Récupération du nom du fichier d'entrée
file_name = recuperate(file_name_path)


file_type_path = "./file_type.txt"
# Récupération du nom du fichier d'entrée
file_type = recuperate(file_type_path)
if 'PATIENT_NAME_ENGLISH' in df.columns:
    df[['FirstName','LastName']] = df['PATIENT_NAME_ENGLISH'].str.split(' ', expand=True)
dict_patient = {
            'PatientNumber':'MR_NO',		
            'DateOfBirth':'BIRTHDATE',
            'Gender':'GENDER',
            'Nationality' : 'NATIONALITY',
            'LastName' : 'LastName',  # Mise à jour du LastName
            'FirstName' : 'FirstName',  # Mise à jour du LastName
            'Title' : 'MARITALSTATUS',
            'NationalIdentifier' : 'NATIONALID'
            }

dict_encounter_ed = {
'PatientNumber': 'MR_NO',
'Hospital': 'CLINIC',
'StartDateTime': 'TIME_ARRIVED',
'EndDateTime': 'TIME_COMPLETE',
'EncounterNumber': 'ENCOUNTERID',
'MaritalStatus': 'MARITALSTATUS',
'Extra:DischargeStatus':'DISHARGE',
}

dict_encounter_ip={
'PatientNumber': 'PATIENTID',
'StartDateTime': 'ADMIT_DATE',
'EndDateTime': 'PHYSICAL_DISCHARGE_DATE',
'EncounterNumber': 'ENCOUNTERID',
'MaritalStatus': 'MARITALSTATUS',
'Extra:DischargeStatus':'DISCHARGE_STATUS',
'Extra:DischargeConsultantName':'DOCTOR_NAME.1',
'Extra:Country':'RESIDENCECOUNTRY',
'AdmittingConsultant':'DOCTOR_ID',
'Extra:AdmittingConsultantName':'DOCTOR_NAME',
'AdmittingConsultantSpecialty' :'SPECIALTY',
'AdmitWard':'WARD_NAME'
}

dict_encounter_op={
'PatientNumber': 'MR_NO',
'Hospital': 'CLINIC',
'StartDateTime': 'TIME_ARRIVED',
'EndDateTime': 'TIME_COMPLETE',
'EncounterNumber': 'ENCOUNTERID',
'MaritalStatus': 'MARITALSTATUS',
'Extra:DischargeStatus':'DISHARGE',
}

dict_transfer = {
'PatientNumber': 'REGISTER_ID',
'BedNumber': 'CURRENT_BED',
'EncounterNumber': 'ENCOUNTERID',
'Ward':'CURRENT_WARD',
'StartDateTime': 'START_DATE'
}

dict_diagnosis = {
'PatientNumber': 'MR_NO',
'EncounterNumber': 'ENCOUNTERID',
'DiagnosisCode':'DIAGNOSISCODE',
#'DiagnosisVersion':'DESCRIPTION', ??
'Extra:DiagnosisType':'DIAGNOSISTYPE',
'ConditionOnset':'CONDITIONONSETFLAG',
'Extra:DiagnosisDateTime':'DATE_RECORDED',
}

dict_imaging = {
'PatientNumber': 'MR_NO',
'StartDateTime': 'LINE_ORDER_DATE',
'Quantity': 'UNITS_ORDERED',
'ServiceCode': 'ACTIVITYID',
'EncounterNumber': 'ENCOUNTERID',
'EndDateTime': 'PHYSICAL_DISCHARGE_DATE',
'Extra:ServiceDescription':'DESCRIPTION',
'ServiceGroup':'ACTIVITYTYPE',
#'OrderDateTime': 'LINE_ORDER_DATE',
}

dict_laboratory = {
'PatientNumber': 'MR_NO',
'StartDateTime': 'LINE_ORDER_DATE',
'Quantity': 'UNITS_ORDERED',
'ServiceCode': 'ACTIVITYID',
'EncounterNumber': 'ENCOUNTERID',
'EndDateTime': 'PHYSICAL_DISCHARGE_DATE',
'Extra:ServiceDescription':'DESCRIPTION',
'ServiceGroup':'ACTIVITYTYPE',
#'OrderDateTime': 'LINE_ORDER_DATE',
}

dict_pharmacy = {
'PatientNumber': 'MR_NO',
'StartDateTime': 'TIME_ARRIVED',
'Quantity': 'QUANTITY',
'ServiceCode': 'ACTIVITYID',
'EncounterNumber': 'ENCOUNTERID',
'Extra:ServiceDescription':'DESCRIPTION',
'ServiceGroup':'ACTIVITYTYPE',
'Clinic':'CLINIC',
'OrderDateTime':'LINE_ORDER_DATE'
}

dict_theater ={
'PatientNumber': 'PATIENTID',
'StartDateTime': 'OPERATION_STARTED',
'ServiceCode': 'STAFF_ID',
'EncounterNumber': 'ENCOUNTERID',
'Duration' : 'DURATION_MINUTS',
'EndDateTime':'OPERATION_END',
'Extra:ServiceDescription':'DESCRIPTION',
#'ServiceGroup':'ACTIVITYTYPE',
'Extra:ConsultantName' : 'CONSULTANT',
'Extra:ElectiveOrEmergency':'OR_TYPE',
'Extra:OperationID':'PRODUCTCODE',
'EncounterType' : 'ENCOUNTER_TYPE',
}

# Exécution de la fonction avec le dictionnaire de correspondances
mapping(df, dict_theater, file_name, file_type,warnings_count, rejections_count)






""""
dict_patient = {
    'PATIENTID' : 'PatientNumber',
    #'Hopital 1' : 'Hospital',
    'BIRTHDATE' : 'DateOfBirth',
    'GENDER':'Gender',
    #'' : 'PatientDeceased',
    'DEATH_DATE' : 'DateofDeath',
    #'' :'PlaceOfBirth',
    #'' : 'EthnicOrigin',
    'NATIONALITY' : 'Nationality',
    'PATIENT_NAME': {
        'LastName': re.compile('^(\S+)'),
        'FirstName': re.compile('^\S+\s+(.*)$')
    },
    'MARITALSTATUS' : 'Title',
    #'' : 'MothersName',
    #'' : 'MothersPreName',
    #'' : 'FathersName',
    #'' : 'FathersPreName',
    #'' : 'FamilyDoctor',
    'NATIONALID' : 'NationalID',
    #'' : 'FileDateCreation'
}
"""



"""
dict_patient = OrderedDict()
dict_patient['PATIENTID'] = 'PatientNumber'
dict_patient['BIRTHDATE'] = 'DateOfBirth'
dict_patient['GENDER'] = 'Gender'
dict_patient[''] = 'PatientDeceased'
dict_patient['DEATH_DATE'] = 'DateofDeath'
dict_patient[''] = 'PlaceOfBirth'
dict_patient[''] = 'EthnicOrigin'
dict_patient['NATIONALITY'] = 'Nationality'
dict_patient['PATIENT_NAME'] = {
    'LastName': re.compile('^(\S+)'),
    'FirstName': re.compile('^\S+\s+(.*)$')
}
dict_patient['MARITALSTATUS'] = 'Title'
dict_patient[''] = 'MothersName'
dict_patient[''] = 'MothersPreName'
dict_patient[''] = 'FathersName'
dict_patient[''] = 'FathersPreName'
dict_patient[''] = 'FamilyDoctor'
dict_patient['NATIONALID'] = 'NationalID'
dict_patient[''] = 'FileDateCreation'
"""

"""                    # Si la correspondance est à l'aide d'une regex
                    if isinstance(new_names, dict):
                        # Itération sur chaque correspondance de nom de colonne
                        for new_name, regex in new_names.items():
                            # Extraction de la partie correspondante de la colonne originale avec la regex
                            df[new_name] = df[original_name].str.extract(regex)
                    # Sinon, la correspondance est simple
                    #             # Suppression des colonnes d'origine qui ont été découpées
            df.drop(columns=[name for name in column_mapping.keys() if isinstance(column_mapping[name], dict)], inplace=True)
                    # 
                    # """