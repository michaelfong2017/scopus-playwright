import pandas as pd
import numpy as np
from pathlib import Path

# ================================
# Configuration
# ================================

# Thresholds
THRESHOLD_F08 = 0.3           # Maximum allowed percentage of extra words for F08
SIMILARITY_THRESHOLD = 0.8    # Minimum similarity between two strings for F07

# File Paths
CURRENT_DIR = Path(".")
OVERALL_REFERENCES_PATH = CURRENT_DIR / "overall_references_of_citing.csv"
OVERALL_MISCITATIONS_PATH = CURRENT_DIR / "overall_miscitations.csv"
OUTPUT_BASE_DIR = CURRENT_DIR / "miscitations_by_eid"
OVERALL_FILTERED_PATH = CURRENT_DIR / "overall_filtered.csv"
OVERALL_CLEANED_PATH = CURRENT_DIR / "overall_cleaned.csv"

# ================================
# Load Input CSVs
# ================================

def load_csv(path, required_columns):
    """
    Loads a CSV file and ensures that all required columns are present.
    
    Parameters:
        path (Path): Path to the CSV file.
        required_columns (list): List of required column names.
    
    Returns:
        pd.DataFrame: Loaded DataFrame with required columns.
    """
    try:
        df = pd.read_csv(path)
        print(f"✅ Loaded '{path}' with {len(df)} rows.")
    except FileNotFoundError:
        print(f"❌ File '{path}' not found. Exiting.")
        exit(1)
    except Exception as e:
        print(f"❌ Error loading '{path}': {e}. Exiting.")
        exit(1)
    
    # Ensure required columns exist
    for col in required_columns:
        if col not in df.columns:
            df[col] = np.nan
            print(f"ℹ️ Added missing column '{col}' to DataFrame.")
    
    return df

# Define required columns for each CSV
required_columns_references = [
    "Citing Article EID",
    "Reference EID",
    "Reference Link",
    "Reference Title"  # Assuming there's a title column for F07
]

required_columns_miscitations = [
    "Cited Article Title",
    "Cited Article EID",
    "Cited Article Link",
    "Miscited Article Title",
    "Miscited Article EID",
    "Miscited Article Link",
    "Citing Article Title",
    "Citing Article EID",
    "Citing Article Link"
]

# Load CSVs
references_df = load_csv(OVERALL_REFERENCES_PATH, required_columns_references)
miscitations_df = load_csv(OVERALL_MISCITATIONS_PATH, required_columns_miscitations)

# ================================
# Define Helper Functions
# ================================

def similar(a, b):
    """
    Calculates similarity between two strings based on common and unique words.
    
    Parameters:
        a (str): First string.
        b (str): Second string.
    
    Returns:
        float: Similarity ratio.
    """
    words_a = set(a.split())
    words_b = set(b.split())
    common = words_a.intersection(words_b)
    unique = words_a.union(words_b) - common

    return len(common) if len(unique) == 0 else len(common) / len(unique)

# ================================
# Define Filtering Functions
# ================================

def F04(miscited_article_eid, citing_article_eid, cited_eid, references_df):
    """
    Checks if the miscited article's EID is among the references of the citing article
    and if any field in miscited_downloads/<cited_eid>/<cited_eid>.csv contains 'arxiv'.
    
    Parameters:
        miscited_article_eid (str): EID of the miscited article.
        citing_article_eid (str): EID of the citing article.
        cited_eid (str): EID of the cited article.
        references_df (pd.DataFrame): DataFrame containing references.
    
    Returns:
        bool: True if F04 criteria met, False otherwise.
    """
    if pd.isnull(miscited_article_eid) or pd.isnull(citing_article_eid) or pd.isnull(cited_eid):
        return False

    # Clean EIDs
    miscited_eid_cleaned = miscited_article_eid.strip()
    citing_eid_cleaned = citing_article_eid.strip()
    cited_eid_cleaned = cited_eid.strip()

    # Filter references for the citing article
    citing_refs = references_df[references_df['Citing Article EID'].astype(str).str.strip() == citing_eid_cleaned]

    if citing_refs.empty:
        return False

    # Check if miscited_eid is among the references
    is_referenced = miscited_eid_cleaned in citing_refs['Reference EID'].astype(str).str.strip().values
    if not is_referenced:
        return False

    # Define the path to miscited_downloads/<cited_eid>/<cited_eid>.csv
    miscited_downloads_path = CURRENT_DIR / "miscited_downloads" / cited_eid_cleaned / f"{cited_eid_cleaned}.csv"

    if not miscited_downloads_path.exists():
        print(f"❌ CSV for cited EID {cited_eid_cleaned} not found at {miscited_downloads_path}.")
        return False

    try:
        miscited_df = pd.read_csv(miscited_downloads_path)
    except Exception as e:
        print(f"❌ Error reading {miscited_downloads_path}: {e}")
        return False

    # Check if any field in the DataFrame contains 'arxiv' (case-insensitive)
    contains_arxiv = miscited_df.applymap(lambda x: isinstance(x, str) and "arxiv" in x.lower()).any().any()

    return contains_arxiv

def F07(miscited_article_title, miscited_article_eid, citing_article_eid, similarity_threshold, references_df):
    """
    Checks if the miscited article's title sufficiently matches the referenced title in the citing article's references.
    
    Parameters:
        miscited_article_title (str): Title of the miscited article.
        miscited_article_eid (str): EID of the miscited article.
        citing_article_eid (str): EID of the citing article.
        similarity_threshold (float): Minimum similarity threshold.
        references_df (pd.DataFrame): DataFrame containing references.
    
    Returns:
        bool: True if F07 criteria met (i.e., insufficient similarity), False otherwise.
    """
    if pd.isnull(miscited_article_title) or pd.isnull(miscited_article_eid) or pd.isnull(citing_article_eid):
        return False

    # Clean EIDs
    miscited_eid_cleaned = miscited_article_eid.strip().split("-")[-1]
    citing_eid_cleaned = citing_article_eid.strip()

    # Filter references for the citing article
    citing_refs = references_df[references_df['Citing Article EID'].astype(str).str.strip() == citing_eid_cleaned]

    if citing_refs.empty:
        return True  # If no references, consider F07 as triggered

    # Get the reference title(s) for the miscited_eid
    ref_titles = citing_refs[citing_refs['Reference EID'].astype(str).str.strip() == miscited_eid_cleaned]['Reference Title']

    if ref_titles.empty:
        return True  # If no titles found for miscited_eid, consider F07 as triggered

    # Check similarity
    for title in ref_titles:
        if pd.notnull(title):
            similarity = similar(str(title).lower(), str(miscited_article_title).lower())
            if similarity >= similarity_threshold:
                return False  # Similarity sufficient, do not trigger F07

    return True  # No sufficient similarity found, trigger F07

def F08(cited_article_title, miscited_article_title, threshold):
    """
    Checks if the miscited article has excessive extra words compared to the cited article.
    
    Parameters:
        cited_article_title (str): Title of the cited article.
        miscited_article_title (str): Title of the miscited article.
        threshold (float): Maximum allowed percentage of extra words.
    
    Returns:
        bool: True if excess exceeds threshold, False otherwise.
    """
    if pd.isnull(cited_article_title) or pd.isnull(miscited_article_title):
        return False

    # Normalize titles
    cited_title = str(cited_article_title).lower()
    miscited_title = str(miscited_article_title).lower()

    # Remove the cited article title from the miscited article title
    if cited_title in miscited_title:
        miscited_title_cleaned = miscited_title.replace(cited_title, "").strip()
    else:
        # If not a substring, consider no removal
        miscited_title_cleaned = miscited_title.strip()

    # Calculate the number of words
    numerator = len(miscited_title.split())
    denominator = len(miscited_title.split()) - len(miscited_title_cleaned.split())

    # Avoid division by zero
    if denominator == 0:
        return True  # All words are extra

    # Calculate the extra words ratio
    calc_threshold = (numerator / denominator) - 1

    if calc_threshold > threshold:
        print(f"🔍 F08 Check - Extra Words Ratio: {calc_threshold:.2f} > Threshold: {threshold}")
    else:
        print(f"🔍 F08 Check - Extra Words Ratio: {calc_threshold:.2f} <= Threshold: {threshold}")

    return calc_threshold > threshold

# ================================
# Apply Filters and Save Outputs
# ================================

def process_miscitations(references_df, miscitations_df, output_base_dir, overall_filtered_path, overall_cleaned_path):
    """
    Processes miscitations by applying filters F04, F07, and F08,
    and saves the results into separate CSV files per Cited Article EID.
    Also saves overall_filtered.csv and overall_cleaned.csv in the current directory.
    
    Parameters:
        references_df (pd.DataFrame): DataFrame containing references.
        miscitations_df (pd.DataFrame): DataFrame containing miscitations.
        output_base_dir (Path): Base directory to save filtered and cleaned CSVs.
        overall_filtered_path (Path): Path to save overall_filtered.csv
        overall_cleaned_path (Path): Path to save overall_cleaned.csv
    """
    # Ensure output directory exists
    if not output_base_dir.exists():
        output_base_dir.mkdir(parents=True)
        print(f"✅ Created base output directory '{output_base_dir}'.")
    else:
        print(f"✅ Base output directory '{output_base_dir}' already exists.")

    # Initialize lists to collect overall filtered and cleaned rows
    overall_filtered_rows = []
    overall_cleaned_rows = []

    # Get unique Cited Article EIDs
    cited_eids = miscitations_df['Cited Article EID'].dropna().astype(str).str.strip().unique()

    for cited_eid in cited_eids:
        # Filter rows for the current Cited Article EID
        subset_df = miscitations_df[miscitations_df['Cited Article EID'].astype(str).str.strip() == cited_eid]
        
        # Initialize lists to collect filtered and cleaned rows for this EID
        filtered_rows = []
        cleaned_rows = []

        print(f"\n📄 Processing Cited EID: {cited_eid} with {len(subset_df)} rows.")

        for index, row in subset_df.iterrows():
            miscited_eid = str(row["Miscited Article EID"]).strip()
            citing_eid = str(row["Citing Article EID"]).strip()
            cited_title = row["Cited Article Title"]
            miscited_title = row["Miscited Article Title"]

            # Apply F08
            f08_flag = F08(cited_title, miscited_title, THRESHOLD_F08)

            # Apply F04
            f04_flag = F04(miscited_eid, citing_eid, cited_eid, references_df)

            # Apply F07
            f07_flag = F07(miscited_title, miscited_eid, citing_eid, SIMILARITY_THRESHOLD, references_df)

            # Determine if any filter is triggered
            if f08_flag or f04_flag or f07_flag:
                reasons = []
                if f08_flag:
                    reasons.append("F08")
                if f04_flag:
                    reasons.append("F04")
                if f07_flag:
                    reasons.append("F07")

                filtered_row = row.to_dict()
                filtered_row["Reason"] = "; ".join(reasons)
                filtered_rows.append(filtered_row)
                overall_filtered_rows.append(filtered_row)

                print(f"❌ Row {index + 1} filtered due to: {filtered_row['Reason']}")
            else:
                cleaned_rows.append(row.to_dict())
                overall_cleaned_rows.append(row.to_dict())

        # Define the directory for the current Cited Article EID
        cited_dir = output_base_dir / cited_eid
        if not cited_dir.exists():
            cited_dir.mkdir(parents=True)
            print(f"✅ Created directory '{cited_dir}'.")
        else:
            print(f"✅ Directory '{cited_dir}' already exists.")

        # Save filtered.csv
        if filtered_rows:
            filtered_df = pd.DataFrame(filtered_rows)
            filtered_csv_path = cited_dir / "filtered.csv"
            try:
                filtered_df.to_csv(filtered_csv_path, index=False)
                print(f"✅ Saved filtered data to '{filtered_csv_path}' with {len(filtered_df)} rows.")
            except Exception as e:
                print(f"❌ Error saving '{filtered_csv_path}': {e}")
        else:
            print(f"ℹ️ No filtered rows for Cited EID '{cited_eid}'. Skipping 'filtered.csv'.")

        # Save cleaned.csv
        if cleaned_rows:
            cleaned_df = pd.DataFrame(cleaned_rows)
            cleaned_csv_path = cited_dir / "cleaned.csv"
            try:
                cleaned_df.to_csv(cleaned_csv_path, index=False)
                print(f"✅ Saved cleaned data to '{cleaned_csv_path}' with {len(cleaned_df)} rows.")
            except Exception as e:
                print(f"❌ Error saving '{cleaned_csv_path}': {e}")
        else:
            print(f"ℹ️ No cleaned rows for Cited EID '{cited_eid}'. Skipping 'cleaned.csv'.")

    # After processing all EIDs, save overall_filtered.csv and overall_cleaned.csv
    print("\n📊 Saving overall_filtered.csv and overall_cleaned.csv.")

    # Save overall_filtered.csv
    if overall_filtered_rows:
        overall_filtered_df = pd.DataFrame(overall_filtered_rows)
        try:
            overall_filtered_df.to_csv(overall_filtered_path, index=False)
            print(f"✅ Saved overall filtered data to '{overall_filtered_path}' with {len(overall_filtered_df)} rows.")
        except Exception as e:
            print(f"❌ Error saving '{overall_filtered_path}': {e}")
    else:
        print(f"ℹ️ No rows met the F04, F07, or F08 filtering criteria. 'overall_filtered.csv' not created.")

    # Save overall_cleaned.csv
    if overall_cleaned_rows:
        overall_cleaned_df = pd.DataFrame(overall_cleaned_rows)
        try:
            overall_cleaned_df.to_csv(overall_cleaned_path, index=False)
            print(f"✅ Saved overall cleaned data to '{overall_cleaned_path}' with {len(overall_cleaned_df)} rows.")
        except Exception as e:
            print(f"❌ Error saving '{overall_cleaned_path}': {e}")
    else:
        print(f"ℹ️ No rows passed the F04, F07, and F08 filtering criteria. 'overall_cleaned.csv' not created.")

# ================================
# Main Execution
# ================================

def main():
    process_miscitations(
        references_df=references_df,
        miscitations_df=miscitations_df,
        output_base_dir=OUTPUT_BASE_DIR,
        overall_filtered_path=OVERALL_FILTERED_PATH,
        overall_cleaned_path=OVERALL_CLEANED_PATH
    )
    print("\n🎉 Filtering process completed.")

if __name__ == "__main__":
    main()
