import sys
import pandas as pd
import os
input_file = sys.argv[1]

column_names = [
    "V-DateOfBirth-1",
    "V-Today-1",
    "V-FormatDate-1",
    "V-NotNull-1",
]

output_file = "/home/bachir/Bureau/S8/HAI823I TER/resultats/duplicates_file.csv"
delimiter = ","  # Ajoutez cette ligne pour définir le délimiteur

def should_remove_line(row):
    for column in column_names:
        if column in row and isinstance(row[column], str) and row[column].strip():
            return True, column
    return False, None

def main():
    df = pd.read_csv(input_file)
    
    # Initialize the Rule column
    df['Rejet'] = ""

    # Store the removed rows
    removed_rows = []
    removed_rows_count = {column: 0 for column in column_names}

    # Filter the rows and save the removed ones
    filtered_df = df[df.apply(lambda row: not should_remove_line(row)[0], axis=1)]
    
    for index, row in df.iterrows():
        should_remove, column = should_remove_line(row)
        if should_remove:
            row['Rejet'] = column
            removed_rows.append(row)
            removed_rows_count[column] += 1

    # Save the removed rows to the output file
    if removed_rows:
        removed_df = pd.DataFrame(removed_rows)
        with open(output_file, "a") as f:
            removed_df.to_csv(f, header=f.tell() == 0, index=False, sep=delimiter)

    # Write the filtered DataFrame to stdout
    filtered_df.to_csv(sys.stdout, index=False)
    
    # Créer le répertoire s'il n'existe pas encore
    directory = "/home/bachir/Bureau/S8/HAI823I TER"
    if not os.path.exists(directory):
        os.makedirs(directory)

    # Chemin du fichier CSV de sortie
    output_csv_file = directory + "/nblignes.csv"

    # Ajouter le nombre de lignes dupliquées au fichier de sortie
    with open(output_csv_file, "a") as f:
        for column, count in removed_rows_count.items():
                f.write(f"{column}:, {count}\n")
        f.write(f"Rejet:, {sum(removed_rows_count.values())}\n")

if __name__ == "__main__":
    main()
