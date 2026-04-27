import pandas as pd
import spacy
import os
import time
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError

# Load spaCy NER model
try:
    nlp = spacy.load("en_core_web_md")
except OSError:
    os.system("python -m spacy download en_core_web_md")
    nlp = spacy.load("en_core_web_md")

# --- CONFIGURATION ---
# Use a very specific user_agent to avoid being flagged as a generic bot
USER_AGENT = "genealogy_research_v10_unique_processor"
geolocator = Nominatim(user_agent=USER_AGENT, timeout=10)

def safe_geocode(query, attempts=5):
    """
    Handles Geocoding with exponential backoff for 429 (Rate Limit) 
    and general timeout errors.
    """
    for i in range(attempts):
        try:
            # Respectful delay between every single request
            time.sleep(1.5) 
            return geolocator.geocode(query, addressdetails=True, language="en")
        except GeocoderServiceError as e:
            if "429" in str(e):
                wait_time = (i + 1) * 10  # Wait 10s, 20s, 30s...
                print(f"Rate limited (429). Waiting {wait_time}s for cooldown...")
                time.sleep(wait_time)
            else:
                print(f"Service error for {query}: {e}")
                return None
        except GeocoderTimedOut:
            print(f"Timeout for {query}. Retrying...")
            time.sleep(2)
        except Exception as e:
            print(f"Unexpected error for {query}: {e}")
            return None
    return None

def extract_hierarchy(location_str):
    """
    Performs the heavy lifting: Geocoding and Categorization.
    Returns a dictionary of the POB components and raw coordinates.
    """
    results = {
        "POB_City": None, "POB_County": None, "POB_State": None, 
        "POB_Country": None, "raw_coords": None
    }
    
    clean_str = str(location_str).strip() if pd.notna(location_str) else ""
    if not clean_str or clean_str.lower() in ['nan', '-']:
        return results

    location = safe_geocode(clean_str)
    
    if location and 'address' in location.raw:
        addr = location.raw['address']
        results["POB_Country"] = addr.get('country')
        results["POB_State"] = addr.get('state')
        results["POB_County"] = addr.get('county')
        results["POB_City"] = addr.get('city') or addr.get('town') or addr.get('village')
        results["raw_coords"] = f"{location.latitude}, {location.longitude}"

        # NER Refinement (logic for State+Country or Country only)
        doc = nlp(clean_str)
        entities = [ent.text.strip() for ent in doc.ents if ent.label_ == "GPE"]
        components = [c.strip() for c in clean_str.split(',')]

        if len(entities) == 1 and entities[0].lower() in results["POB_Country"].lower():
            results["POB_City"], results["POB_County"], results["POB_State"] = None, None, None

        if len(components) <= 2:
            if "county" not in clean_str.lower(): results["POB_County"] = None
            if "city" not in clean_str.lower() and not any(comp.lower() in (results["POB_City"] or "").lower() for comp in components):
                results["POB_City"] = None

    return results

def run_optimized_pob_transformation(input_path, output_path):
    if not os.path.exists(input_path):
        print(f"File {input_path} not found.")
        return

    df = pd.read_excel(input_path)
    pob_col = 'Place_of_birth'
    coord_col = 'Birth_coordinates'

    if pob_col not in df.columns or coord_col not in df.columns:
        print(f"Required columns missing in {input_path}.")
        return

    # --- OPTIMIZATION: Process Unique Values Only ---
    unique_pobs = df[pob_col].dropna().unique()
    print(f"Found {len(df)} total rows. Processing {len(unique_pobs)} unique locations...")
    
    lookup_cache = {}
    for idx, loc in enumerate(unique_pobs):
        lookup_cache[loc] = extract_hierarchy(loc)
        if idx % 5 == 0:
            print(f"Progress: {idx}/{len(unique_pobs)} unique locations processed.")

    # --- MAPPING DATA BACK TO MAIN DATAFRAME ---
    print("Mapping results back to original data...")
    
    # Initialize new columns
    df["POB_City"] = None
    df["POB_County"] = None
    df["POB_State"] = None
    df["POB_Country"] = None

    for idx, row in df.iterrows():
        pob_val = row[pob_col]
        if pob_val in lookup_cache:
            res = lookup_cache[pob_val]
            df.at[idx, "POB_City"] = res["POB_City"]
            df.at[idx, "POB_County"] = res["POB_County"]
            df.at[idx, "POB_State"] = res["POB_State"]
            df.at[idx, "POB_Country"] = res["POB_Country"]

            # Update Birth_coordinates only if currently empty
            is_empty = pd.isna(row[coord_col]) or str(row[coord_col]).strip() in ["", "-", "nan"]
            if is_empty and res["raw_coords"]:
                df.at[idx, coord_col] = res["raw_coords"]

    # --- REORDERING COLUMNS ---
    # Put POB_ columns next to Place_of_birth
    target_idx = df.columns.get_loc(pob_col) + 1
    cols_to_move = ["POB_Country", "POB_State", "POB_County", "POB_City"]
    
    # Get all columns except the ones we are moving
    remaining_cols = [c for c in df.columns if c not in cols_to_move]
    
    # Reassemble with the specific order
    new_column_order = remaining_cols[:target_idx] + cols_to_move + remaining_cols[target_idx:]
    df = df[new_column_order]

    df.to_excel(output_path, index=False)
    print(f"Success! Optimized file saved as: {output_path}")

if __name__ == "__main__":
    FILE_NAME = 'USCTs_Connecticut_rev_03_copy.xlsx'
    OUTPUT_NAME = 'USCTs_Connecticut_rev_04.xlsx'
    
    run_optimized_pob_transformation(FILE_NAME, OUTPUT_NAME)