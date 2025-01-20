import os
import pandas as pd
import numpy as np
import glob
from pathlib import Path

# Configuration
threshold = 0.3  # Maximum % of extra words allowed

# Define directory paths
output_dir_path = "miscitations_by_eid"
overall_miscitations_path = Path("overall_miscitations.csv")
overall_filtered_path = Path("overall_filtered.csv")
overall_cleaned_path = Path("overall_cleaned.csv")

# Define filtering functions

def F08(cited_article_title, miscited_article_title, threshold):
    """
    Checks if the miscited article has excessive extra words compared to the cited article.

    Parameters:
        cited_article_title (str): Title of the cited article.
        miscited_article_title (str): Title of the miscited article.
        threshold (float): Maximum allowed percentage of extra words.

    Returns:
        bool: True if the miscited article exceeds the extra words threshold, False otherwise.
    """
    if pd.isnull(miscited_article_title):
        return False

    # Convert titles to lowercase for case-insensitive comparison
    cited_article_title = str(cited_article_title).lower()
    miscited_article_title = str(miscited_article_title).lower()

    # Remove the cited article title from the miscited article title
    miscited_article_title_cleaned = miscited_article_title.replace(cited_article_title, "").strip()
    
    # Calculate the number of words
    numerator = len(miscited_article_title.split())
    denominator = len(miscited_article_title.split()) - len(miscited_article_title_cleaned.split())

    # Avoid division by zero
    if denominator == 0:
        return True  # All words are extra

    # Calculate the extra words threshold
    calc_threshold = (numerator / denominator) - 1

    return calc_threshold > threshold

# Function to process individual miscitations.csv files
def process_eid_miscitations(eid_folder, threshold):
    """
    Processes a single miscitations.csv file for a given EID,
    applies the F08 filtering rule, and writes filtered and cleaned CSVs.

    Parameters:
        eid_folder (Path): Path to the EID-specific folder containing miscitations.csv.
        threshold (float): Maximum allowed percentage of extra words for F08.
    """
    miscitations_csv_path = eid_folder / "miscitations.csv"
    if not miscitations_csv_path.exists():
        print(f"❌ miscitations.csv not found in {eid_folder}. Skipping.")
        return

    try:
        df = pd.read_csv(miscitations_csv_path)
    except Exception as e:
        print(f"❌ Error reading {miscitations_csv_path}: {e}")
        return

    # Initialize lists to collect filtered and cleaned rows
    filtered_rows = []
    cleaned_rows = []

    total_rows = len(df)
    print(f"📄 Processing {miscitations_csv_path} with {total_rows} rows.")

    for index, row in df.iterrows():
        # Skip rows where the Cited Article EID is missing
        if pd.isnull(row["Cited Article EID"]):
            continue

        cited_article_title = row["Cited Article Title"]
        miscited_article_title = row["Miscited Article Title"]

        # Apply F08 filtering rule
        try:
            if F08(cited_article_title, miscited_article_title, threshold):
                # Add Reason for filtering
                filtered_row = row.to_dict()
                filtered_row["Reason"] = "F08"
                filtered_rows.append(filtered_row)
            else:
                cleaned_rows.append(row.to_dict())
        except Exception as e:
            print(f"❌ Error processing row {index + 1} in {miscitations_csv_path}: {e}")
            continue

    # Convert lists to DataFrames
    initializer_keys = [
        "Cited Article Title",
        "Cited Article EID",
        "Cited Article Link",
        "Miscited Article Title",
        "Miscited Article EID",
        "Miscited Article Link",
        "Citing Article Title",
        "Citing Article EID",
        "Citing Article Link",
    ]

    if filtered_rows:
        df_filtered = pd.DataFrame(filtered_rows, columns=initializer_keys + ["Reason"])
        filtered_csv_path = eid_folder / "filtered.csv"
        try:
            df_filtered.to_csv(filtered_csv_path, index=False)
            print(f"✅ Saved filtered data to {filtered_csv_path} with {len(df_filtered)} rows.")
        except Exception as e:
            print(f"❌ Error saving filtered.csv in {eid_folder}: {e}")
    else:
        print(f"ℹ️ No rows met the F08 filtering criteria in {miscitations_csv_path}.")

    if cleaned_rows:
        df_cleaned = pd.DataFrame(cleaned_rows, columns=initializer_keys)
        cleaned_csv_path = eid_folder / "cleaned.csv"
        try:
            df_cleaned.to_csv(cleaned_csv_path, index=False)
            print(f"✅ Saved cleaned data to {cleaned_csv_path} with {len(df_cleaned)} rows.")
        except Exception as e:
            print(f"❌ Error saving cleaned.csv in {eid_folder}: {e}")
    else:
        print(f"ℹ️ No rows passed the F08 filtering criteria in {miscitations_csv_path}.")

# Function to process the overall_miscitations.csv
def process_overall_miscitations(overall_miscitations_path, threshold):
    """
    Processes the overall_miscitations.csv file,
    applies the F08 filtering rule, and writes overall_filtered.csv and overall_cleaned.csv.

    Parameters:
        overall_miscitations_path (Path): Path to the overall_miscitations.csv file.
        threshold (float): Maximum allowed percentage of extra words for F08.
    """
    if not overall_miscitations_path.exists():
        print(f"❌ {overall_miscitations_path} not found. Skipping overall processing.")
        return

    try:
        df = pd.read_csv(overall_miscitations_path)
    except Exception as e:
        print(f"❌ Error reading {overall_miscitations_path}: {e}")
        return

    # Initialize lists to collect filtered and cleaned rows
    filtered_rows = []
    cleaned_rows = []

    initializer_keys = [
        "Cited Article Title",
        "Cited Article EID",
        "Cited Article Link",
        "Miscited Article Title",
        "Miscited Article EID",
        "Miscited Article Link",
        "Citing Article Title",
        "Citing Article EID",
        "Citing Article Link",
    ]

    total_rows = len(df)
    print(f"📄 Processing {overall_miscitations_path} with {total_rows} rows.")

    for index, row in df.iterrows():
        # Skip rows where the Cited Article EID is missing
        if pd.isnull(row["Cited Article EID"]):
            continue

        cited_article_title = row["Cited Article Title"]
        miscited_article_title = row["Miscited Article Title"]

        # Apply F08 filtering rule
        try:
            if F08(cited_article_title, miscited_article_title, threshold):
                # Add Reason for filtering
                filtered_row = row.to_dict()
                filtered_row["Reason"] = "F08"
                filtered_rows.append(filtered_row)
            else:
                cleaned_rows.append(row.to_dict())
        except Exception as e:
            print(f"❌ Error processing row {index + 1} in {overall_miscitations_path}: {e}")
            continue

    # Save the filtered and cleaned DataFrames
    if filtered_rows:
        df_filtered = pd.DataFrame(filtered_rows, columns=initializer_keys + ["Reason"])
        try:
            df_filtered.to_csv(overall_filtered_path, index=False)
            print(f"✅ Saved overall filtered data to {overall_filtered_path} with {len(df_filtered)} rows.")
        except Exception as e:
            print(f"❌ Error saving overall_filtered.csv: {e}")
    else:
        print(f"ℹ️ No rows met the F08 filtering criteria in {overall_miscitations_path}.")

    if cleaned_rows:
        df_cleaned = pd.DataFrame(cleaned_rows, columns=initializer_keys)
        try:
            df_cleaned.to_csv(overall_cleaned_path, index=False)
            print(f"✅ Saved overall cleaned data to {overall_cleaned_path} with {len(df_cleaned)} rows.")
        except Exception as e:
            print(f"❌ Error saving overall_cleaned.csv: {e}")
    else:
        print(f"ℹ️ No rows passed the F08 filtering criteria in {overall_miscitations_path}.")

def main():
    # Ensure the main output directory exists
    output_dir = Path(output_dir_path)
    if not output_dir.exists():
        print(f"❌ Output directory '{output_dir_path}' does not exist.")
        return

    # Process each EID-specific miscitations.csv
    eid_folders = [f for f in output_dir.iterdir() if f.is_dir()]

    for eid_folder in eid_folders:
        process_eid_miscitations(eid_folder, threshold)

    # Process the overall_miscitations.csv
    process_overall_miscitations(overall_miscitations_path, threshold)

if __name__ == "__main__":
    main()
