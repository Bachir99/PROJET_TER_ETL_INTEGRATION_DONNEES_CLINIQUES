import sys
import pandas as pd
import os
# Read CSV data from standard input
input_csv = pd.read_csv(sys.stdin)

# Replace 'PatientNumber' with the name of the column you want to use for deduplication
deduplicated_df = input_csv.drop_duplicates(subset='PatientNumber', keep='first')

# Write the deduplicated DataFrame to standard output as CSV
deduplicated_df.to_csv(sys.stdout, index=False)

# Find the duplicate rows
duplicates_df = input_csv[input_csv.duplicated(subset='PatientNumber', keep='first')]

# Add a column to the duplicates DataFrame indicating that these rows are duplicates
duplicates_df = duplicates_df.assign(Rejet='duplication')

# Save the duplicate rows to a CSV file
duplicates_file_path = '/home/bachir/Bureau/S8/HAI823I TER/resultats/duplicates_file.csv'
duplicates_df.to_csv(duplicates_file_path, index=False)

# Créer le répertoire s'il n'existe pas encore
directory = "/home/bachir/Bureau/S8/HAI823I TER"
if not os.path.exists(directory):
    os.makedirs(directory)

# Chemin du fichier CSV de sortie
output_csv_file = directory + "/output.csv"

# Compter le nombre de lignes dupliquées
nb_lignes_dupliquees = len(input_csv) - len(deduplicated_df)

# Ajouter le nombre de lignes dupliquées au fichier de sortie
with open(output_csv_file, "a") as f:
    f.write("\nduplication:,"+str(nb_lignes_dupliquees) + "\n")

