import sys
import pandas as pd
import datetime

column_name = sys.argv[1]

today = datetime.datetime.now().strftime('%Y-%m-%d')

input_csv = pd.read_csv(sys.stdin)

for index, row in input_csv.iterrows():
    if pd.isna(row[column_name]):
        input_csv.at[index, column_name] = today
        break

input_csv.to_csv(sys.stdout, index=False)
