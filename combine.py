import os
import pandas as pd
from pathlib import Path

def create_miscitations_csv(
    eid_with_titles_path='eid_with_titles.csv',
    miscited_downloads_dir='miscited_downloads',
    citing_downloads_dir='citing_downloads',
    output_dir='miscitations_by_eid',
    output_file='overall_miscitations.csv',  # Overall CSV file name
    per_eid_file='miscitations.csv'  # Per EID CSV file name
):
    # Ensure output directory exists
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    # Load eid_with_titles.csv into a DataFrame
    try:
        eid_df = pd.read_csv(eid_with_titles_path)
    except FileNotFoundError:
        print(f"Error: '{eid_with_titles_path}' not found.")
        return

    # Create a dictionary mapping EID to Title and Link
    eid_dict = {}
    for _, row in eid_df.iterrows():
        eid = row['EID']
        title = row['Title']
        link = row.get('Abstract')  # Assuming 'Abstract' contains the link
        if pd.isna(link):
            link = ''
        eid_dict[eid] = {'Title': title, 'Link': link}

    # Prepare list to collect all rows for the overall CSV
    all_rows = []

    # Iterate through miscited_downloads directory recursively
    miscited_path = Path(miscited_downloads_dir)
    if not miscited_path.exists():
        print(f"Error: '{miscited_downloads_dir}' directory does not exist.")
        return

    for cited_eid_csv in miscited_path.glob('**/*.csv'):
        cited_eid = cited_eid_csv.stem  # Filename without .csv
        if cited_eid.lower() == 'status':
            continue  # Skip files named 'status.csv' if any

        cited_info = eid_dict.get(cited_eid, {'Title': '', 'Link': ''})
        cited_title = cited_info['Title']
        cited_link = cited_info['Link']

        # Read miscited CSV for the cited EID
        try:
            miscited_df = pd.read_csv(cited_eid_csv)
        except Exception as e:
            print(f"Warning: Could not read '{cited_eid_csv}'. Skipping. Error: {e}")
            continue

        # Prepare list to collect rows for the current cited EID
        per_eid_rows = []

        # Iterate through each miscited EID
        for _, miscited_row in miscited_df.iterrows():
            miscited_eid = miscited_row.get('EID')
            if pd.isna(miscited_eid):
                continue
            miscited_eid = str(miscited_eid).strip()
            miscited_title = miscited_row.get('Title', '')
            # Construct the Miscited Article Link
            miscited_link = f"https://www.scopus.com/record/display.url?eid={miscited_eid}&origin=resultslist"

            # Define path to citing CSV
            citing_csv_path = Path(citing_downloads_dir) / cited_eid / miscited_eid / f"{miscited_eid}.csv"
            if not citing_csv_path.exists():
                print(f"Warning: Citing CSV '{citing_csv_path}' does not exist. Skipping.")
                continue

            # Read citing CSV
            try:
                citing_df = pd.read_csv(citing_csv_path)
            except Exception as e:
                print(f"Warning: Could not read '{citing_csv_path}'. Skipping. Error: {e}")
                continue

            # Iterate through citing articles
            for _, citing_row in citing_df.iterrows():
                citing_title = citing_row.get('Title', '')
                citing_eid = citing_row.get('EID', '')
                if pd.isna(citing_eid):
                    continue
                citing_eid = str(citing_eid).strip()
                # Construct the Citing Article Link
                citing_link = f"https://www.scopus.com/record/display.url?eid={citing_eid}&origin=resultslist"

                # Prepare the row dictionary
                row = {
                    'Cited Article Title': cited_title,
                    'Cited Article EID': cited_eid,
                    'Cited Article Link': cited_link,
                    'Miscited Article Title': miscited_title,
                    'Miscited Article EID': miscited_eid,
                    'Miscited Article Link': miscited_link,
                    'Citing Article Title': citing_title,
                    'Citing Article EID': citing_eid,
                    'Citing Article Link': citing_link
                }

                # Append to overall rows
                all_rows.append(row)

                # Append to per EID rows
                per_eid_rows.append(row)

        # After processing all miscited EIDs for the current cited EID,
        # save the per EID CSV if there are any rows
        if per_eid_rows:
            # Create a subfolder for the current cited EID
            cited_eid_folder = Path(output_dir) / cited_eid
            cited_eid_folder.mkdir(parents=True, exist_ok=True)

            # Create DataFrame for the current cited EID
            per_eid_df = pd.DataFrame(per_eid_rows, columns=[
                'Cited Article Title',
                'Cited Article EID',
                'Cited Article Link',
                'Miscited Article Title',
                'Miscited Article EID',
                'Miscited Article Link',
                'Citing Article Title',
                'Citing Article EID',
                'Citing Article Link'
            ])

            # Define the path for the per EID CSV
            per_eid_csv_path = cited_eid_folder / per_eid_file

            # Save to CSV
            per_eid_df.to_csv(per_eid_csv_path, index=False)
            print(f"Miscitations for EID '{cited_eid}' have been saved to '{per_eid_csv_path}'.")

    # After processing all cited EIDs, create the overall CSV if desired
    if all_rows:
        output_df = pd.DataFrame(all_rows, columns=[
            'Cited Article Title',
            'Cited Article EID',
            'Cited Article Link',
            'Miscited Article Title',
            'Miscited Article EID',
            'Miscited Article Link',
            'Citing Article Title',
            'Citing Article EID',
            'Citing Article Link'
        ])

        # Save the overall CSV
        overall_csv_path = output_file
        output_df.to_csv(overall_csv_path, index=False)
        print(f"Overall miscitations CSV has been created at '{overall_csv_path}'.")
    else:
        print("No miscitations found. No CSV files created.")

if __name__ == "__main__":
    create_miscitations_csv()
