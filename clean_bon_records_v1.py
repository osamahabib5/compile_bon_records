import pandas as pd
import spacy
import re
from thefuzz import fuzz, process

# Load spaCy
try:
    nlp = spacy.load("en_core_web_sm")
except OSError:
    from spacy.cli import download
    download("en_core_web_sm")
    nlp = spacy.load("en_core_web_sm")

# --- 1. Load Files ---
FILE_PATH = 'Consolidated_Directory.xlsx'
df = pd.read_excel(FILE_PATH)

# --- 2. Helper Logic ---

def get_combined_name(first, last):
    """Joins names into a single lowercase string for comparison."""
    f = str(first).strip().lower() if pd.notna(first) and str(first) != "-" else ""
    l = str(last).strip().lower() if pd.notna(last) and str(last) != "-" else ""
    return f"{f} {l}".strip()

def find_best_truth_match(inc_row, truth_df):
    """
    Implements the tiered matching logic:
    1. Fuzzy Ship Match
    2. Tie-break with Commander if needed
    """
    inc_ship = str(inc_row['Ship_Name']).lower()
    inc_cmd = str(inc_row['Commander']).lower() if pd.notna(inc_row['Commander']) else ""

    # Get unique correct ship names
    truth_ships = truth_df['Ship_Name'].unique()
    
    # Fuzzy match the ship name (handles Amities vs Amity's)
    best_ship, ship_score = process.extractOne(inc_ship, truth_ships, scorer=fuzz.token_sort_ratio)

    if ship_score < 85: # Threshold for "somewhat similar"
        return None

    # Filter truth records by that ship
    potential_matches = truth_df[truth_df['Ship_Name'] == best_ship]

    # Tie-breaker: If ship exists in multiple books/entries, match Commander
    if len(potential_matches['Book'].unique()) > 1 or len(potential_matches) > 1:
        truth_commanders = potential_matches['Commander'].unique()
        best_cmd, cmd_score = process.extractOne(inc_cmd, truth_commanders, scorer=fuzz.token_sort_ratio)
        return potential_matches[potential_matches['Commander'] == best_cmd].iloc[0]
    
    return potential_matches.iloc[0]

# --- 3. Execution ---

# Identify Truth (Book is populated) vs Incorrect (Book is blank)
truth_mask = df['Book'].notna() & (df['Book'] != "") & (df['Book'] != "-")
df_truth = df[truth_mask].copy()
df_inc = df[~truth_mask].copy()

indices_to_delete = []

print(f"🔍 Analyzing {len(df_inc)} records with blank 'Book' entries...")

for idx, inc_row in df_inc.iterrows():
    match_row = find_best_truth_match(inc_row, df_truth)

    if match_row is not None:
        # Step 1: Combine names for processing
        inc_full_name = get_combined_name(inc_row['First_Name'], inc_row['Surname'])
        truth_full_name = get_combined_name(match_row['First_Name'], match_row['Surname'])

        # Step 2: Compare Names (Fuzzy check for typos like William Holchapan)
        name_score = fuzz.token_sort_ratio(inc_full_name, truth_full_name)

        if name_score > 90:
            # SCENARIO: Duplicate found -> Delete
            indices_to_delete.append(idx)
        else:
            # SCENARIO: Ship/Commander match but Name is different -> Update & Salvage
            # Logic: Populate Book, Ship_Notes, and Commander from Truth
            df.at[idx, 'Book'] = match_row['Book']
            df.at[idx, 'Ship_Notes'] = match_row['Ship_Notes']
            df.at[idx, 'Commander'] = match_row['Commander']
            # Note: Rest of columns like First_Name/Surname remain unchanged

# --- 4. Final Save ---
df_final = df.drop(indices_to_delete)
df_final.to_excel('Consolidated_Directory_v4.xlsx', index=False)

print(f"✨ Process Complete.")
print(f"   - Records deleted (duplicates): {len(indices_to_delete)}")
print(f"   - Records salvaged (missing info populated): {len(df_inc) - len(indices_to_delete)}")