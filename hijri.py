import sys
import csv
from convertdate import islamic, gregorian
import re

def hijri_to_gregorian_converter(value):
    date_value = value.split(' ')[0]  # Get the date part of the value (ignoring the time part)
    hijri_parts = re.split(r'[/-]', date_value)
    if  len(hijri_parts[2]) == 4 and int(hijri_parts[2]) < 1500:
        hijri_day = int(hijri_parts[0])
        hijri_month = int(hijri_parts[1])
        hijri_year = int(hijri_parts[2])
        gregorian_date = islamic.to_gregorian(hijri_year, hijri_month, hijri_day)
        formatted_gregorian_date = f"{gregorian_date[0]:04d}-{gregorian_date[1]:02d}-{gregorian_date[2]:02d} 00:00:00"
        return formatted_gregorian_date
    else:
        return value

if __name__ == '__main__':
    csv_reader = csv.reader(sys.stdin)
    csv_writer = csv.writer(sys.stdout)

    header = next(csv_reader)
    birth_of_date_idx = header.index('BIRTHDATE')
    csv_writer.writerow(header)

    for row in csv_reader:
        row[birth_of_date_idx] = hijri_to_gregorian_converter(row[birth_of_date_idx])
        csv_writer.writerow(row)