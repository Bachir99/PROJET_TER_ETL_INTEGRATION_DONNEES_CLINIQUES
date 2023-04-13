import sys
import os
import pandas as pd
import re
import datetime

# Récupérer le chemin du fichier Excel à partir des arguments de ligne de commande
input_excel_file = sys.argv[1]

# Lire le fichier Excel
df = pd.read_excel(input_excel_file)

dic = {'January' : '01', 'February' :'02', 'March': '03', 'April': '04', 'May': '05', 'June': '06',
       'July': '07', 'August': '08', 'September': '09', 'October': '10', 'November': '11', 'December': '12'}


for i in range(len(df.index)):
    for j in range(len(df.columns)):
        cell_value = str(df.iloc[i, j])
        if isinstance(cell_value, str) and re.match(r'^\d{2}-\d{2}-\d{4}$', cell_value):
            # La valeur de la cellule correspond à l'expression régulière

            # Modifier le format de la chaîne de caractères
            new_value = re.sub(r'^(\d{2})-(\d{2})-(\d{4})$', r'\3-\2-\1 00:00:00', cell_value)
            
            # Assigner la nouvelle valeur de la cellule
            df.iloc[i, j] = new_value
        #august 03,2019
        elif isinstance(cell_value, str) and re.match(r'^[A-Za-z]+\s\d{1,2},\d{4}$', cell_value):            
            month, day_year = cell_value.split(' ')
            day, year = day_year.split(',')
            month_num = dic.get(month.capitalize())
            if month_num is not None:
                new_date_str = f"{year}-{month_num.zfill(2)}-{day.zfill(2)} 00:00:00"
                df.iloc[i, j] = new_date_str
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
    
# Save the DataFrame to my output flowfile
df.to_csv(sys.stdout.buffer, index=False)
