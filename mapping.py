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
        content = file.read()
    return content

def mapping(df, choix, column_mapping, nom_fichier, type_fichier, rejections_count):
    
    #Les mettre en majuscule
    column_mapping = {key.upper(): value for key, value in column_mapping.items()}
    
    
    if len(column_mapping)==0:
        create_excel(df,len(df),0,rejections_count)
    
    else :
        
        dict_patient = {
            'PatientNumber':'MRN Number',		
            'DateOfBirth':'DateOfBirth',
            'Gender':'Gender',
            'Extra:PatientDeceased' : 'PatientDeceased',
            'Extra:DateofDeath' : 'DateofDeath',
            'Extra:PlaceOfBirth' : 'PlaceOfBirth',	
            'EthnicOrigin' : 'EthnicOrigin',
            'Nationality' : 'Extra:Nationality',
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

        
        # Lecture du fichier CSV à partir de l'entrée standard (stdin)
        #df = pd.read_csv(sys.stdin, delimiter=',')

        # Réorganiser les colonnes du DataFrame en suivant l'ordre défini dans le dictionnaire
        df = df[column_mapping.values()]
        
        if choix == 1 : #Un fichier Patient
            # Itération sur chaque colonne du dataframe
            for new_names, original_name in column_mapping.items():
                # Vérifier si la clé (le nom de colonne) est présente dans le dictionnaire
                if original_name in df.columns:
                    # Renommage de la colonne
                    df.rename(columns={original_name: new_names}, inplace=True)
            
            # Recherche du nom de l'hôpital dans le nom du fichier
            hopital_name = re.search('(Hop.*)\.csv', nom_fichier)
            
            # Création de la colonne "Hospital" avec le nom de l'hôpital
            df.insert(1, 'Hospital', hopital_name.group(1))

            # Écriture du fichier CSV résultant sur la sortie standard (stdout)
            df.to_csv(sys.stdout, sep=',', index=False)
        
        elif choix == 2 : #Un fichier Encounter
            # Itération sur chaque colonne du dataframe
            for new_names, original_name in column_mapping.items():
                # Vérifier si la clé (le nom de colonne) est présente dans le dictionnaire
                if original_name in df.columns:
                    # Renommage de la colonne
                    df.rename(columns={original_name: new_names}, inplace=True)
            
            # Recherche du nom de l'hôpital dans le nom du fichier
            encounterType = re.search('OP|IP|ED', nom_fichier)
            
            # Création de la colonne "Hospital" avec le nom de l'hôpital
            df.insert(1, 'EncounterType', encounterType)

            # Écriture du fichier CSV résultant sur la sortie standard (stdout)
            df.to_csv(sys.stdout, sep=',', index=False)            
                
        elif choix == 3 : #un fichier TRANSFER
            
            # Itération sur chaque colonne du dataframe
            for new_names, original_name in column_mapping.items():
                # Vérifier si la clé (le nom de colonne) est présente dans le dictionnaire
                if original_name in df.columns:
                    # Renommage de la colonne
                    df.rename(columns={original_name: new_names}, inplace=True)
            
            # Recherche du nom de l'hôpital dans le nom du fichier
            hopital_name = re.search('(Hop.*)\.csv', nom_fichier)
            
            # Création de la colonne "Hospital" avec le nom de l'hôpital
            df.insert(1, 'Hospital', hopital_name.group(1))

            #TODO : BedNumber et RoomNumber c'est des colonnes qu'on doit en déduire
            
            
            # Écriture du fichier CSV résultant sur la sortie standard (stdout)
            df.to_csv(sys.stdout, sep=',', index=False)            
        
        elif choix == 4 : #un fichier DIAGNOSIS
            
            # Itération sur chaque colonne du dataframe
            for new_names, original_name in column_mapping.items():
                # Vérifier si la clé (le nom de colonne) est présente dans le dictionnaire
                if original_name in df.columns:
                    # Renommage de la colonne
                    df.rename(columns={original_name: new_names}, inplace=True)
            
            # Recherche du nom de l'hôpital dans le nom du fichier
            hopital_name = re.search('(Hop.*)\.csv', nom_fichier)
            
            # Création de la colonne "Hospital" avec le nom de l'hôpital
            df.insert(1, 'Hospital', hopital_name.group(1))            
            
            # Écriture du fichier CSV résultant sur la sortie standard (stdout)
            df.to_csv(sys.stdout, sep=',', index=False)          
        elif choix == 5 : #Un fichier procédure
            
            # Itération sur chaque colonne du dataframe
            for new_names, original_name in column_mapping.items():
                # Vérifier si la clé (le nom de colonne) est présente dans le dictionnaire
                if original_name in df.columns:
                    # Renommage de la colonne
                    df.rename(columns={original_name: new_names}, inplace=True)
            
            # Recherche du nom de l'hôpital dans le nom du fichier
            hopital_name = re.search('(Hop.*)\.csv', nom_fichier)
            
            # Création de la colonne "Hospital" avec le nom de l'hôpital
            df.insert(1, 'Hospital', hopital_name.group(1))            
            
            # Écriture du fichier CSV résultant sur la sortie standard (stdout)
            df.to_csv(sys.stdout, sep=',', index=False) 

        elif choix == 6 :
            
            # Itération sur chaque colonne du dataframe
            for new_names, original_name in column_mapping.items():
                # Vérifier si la clé (le nom de colonne) est présente dans le dictionnaire
                if original_name in df.columns:
                    # Renommage de la colonne
                    df.rename(columns={original_name: new_names}, inplace=True)
            
            # Recherche du nom de l'hôpital dans le nom du fichier
            hopital_name = re.search('(Hop.*)\.csv', nom_fichier)
            
            # Création de la colonne "Hospital" avec le nom de l'hôpital
            df.insert(1, 'Hospital', hopital_name.group(1))            
            
            servicingDepartment = re.search('Imaging|Laboratory|Theater|Pharmacy|Nutrition', nom_fichier)
            
            # Création de la colonne "Hospital" avec le nom de l'hôpital
            df.insert(7, 'ServicingDepartment', servicingDepartment)               
            
            # Écriture du fichier CSV résultant sur la sortie standard (stdout)
            df.to_csv(sys.stdout, sep=',', index=False) 
            
            


dict_patient = {
'PatientNumber':'MRN Number',		
'DateOfBirth':'DateOfBirth',
'Gender':'Gender',
'Extra:PatientDeceased' : 'PatientDeceased',
'Extra:DateofDeath' : 'DateofDeath',
'Extra:PlaceOfBirth' : 'PlaceOfBirth',	
'EthnicOrigin' : 'EthnicOrigin',
'Nationality' : 'Extra:Nationality',
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



df = pd.read_csv(sys.stdin)
rejections_count = {"Absence MandatoryField":len(df)}

file_name_path = "./file_name.txt"
# Récupération du nom du fichier d'entrée
file_name = recuperate(file_name_path)


file_type_path = "./file_type.txt"
# Récupération du nom du fichier d'entrée
file_type = recuperate(file_type_path)

# Exécution de la fonction avec le dictionnaire de correspondances
mapping(df, 1, dict_patient, file_name, file_type,rejections_count)






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