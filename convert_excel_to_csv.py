import sys
import os
import pandas as pd

# Récupérer le chemin du fichier Excel à partir des arguments de ligne de commande
input_excel_file = sys.argv[1]

# Lire le fichier Excel
df = pd.read_excel(input_excel_file)

# Compter le nombre de lignes initiales
nb_lignes_initiales = len(df)

# Créer le répertoire s'il n'existe pas encore
directory = "/home/bachir/Bureau/S8/HAI823I TER"
if not os.path.exists(directory):
    os.makedirs(directory)

# Chemin du fichier CSV de sortie
output_csv_file = directory + "/nblignes.csv"

# Écrire le nombre de lignes initiales dans le fichier CSV de sortie
with open(output_csv_file, "w") as f:
    f.write("initiales:,"+str(nb_lignes_initiales))
    
# Save the DataFrame to a CSV file
df.to_csv(sys.stdout.buffer, index=False)
