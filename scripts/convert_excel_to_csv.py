import sys
import os
import pandas as pd
import re
import datetime
import tempfile
import openpyxl

# Create a temporary file
#with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as temp:
#    for line in sys.stdin.buffer:
#        temp.write(line)
#    temp_filename = temp.name

# Now you can use pandas to read the file
#df = pd.read_excel(temp_filename, engine='openpyxl')

input_excel_file = sys.argv[1]
df = pd.read_excel(input_excel_file, dtype=str)

dic = {'January' : '01', 'February' :'02', 'March': '03', 'April': '04', 'May': '05', 'June': '06',
       'July': '07', 'August': '08', 'September': '09', 'October': '10', 'November': '11', 'December': '12'}


for i in range(len(df.index)):
    for j in range(len(df.columns)):
        cell_value = str(df.iloc[i, j])
        # La valeur de la cellule correspond à l'expression régulière
        if isinstance(cell_value, str) and re.match(r'^\d{2}-\d{2}-\d{4}$', cell_value):
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
    
# Save the DataFrame to my output flowfile
df.to_csv(sys.stdout.buffer, index=False)
