import pandas as pd
import spacy
from geopy.geocoders import Nominatim
import time
import re

# --- 1. SETUP ---
try:
    nlp = spacy.load("en_core_web_md")
except OSError:
    from spacy.cli import download
    download("en_core_web_md")
    nlp = spacy.load("en_core_web_md")

# Initialize Geocoder
geolocator = Nominatim(user_agent="historical_data_validator_v8")

INPUT_FILE = 'notes_sample.xlsx'
OUTPUT_FILE = 'Extracted_Geographic_Validation.xlsx'

# --- 2. LOCAL KNOWLEDGE FOR CLASSIFICATION ---
COMMON_COUNTRIES = {"United States", "Great Britain", "UK", "Canada", "Jamaica", "Bahamas", "Bermuda", "Nova Scotia"}
US_STATES = {
    "Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado", "Connecticut", "Delaware", "Florida", 
    "Georgia", "Hawaii", "Idaho", "Illinois", "Indiana", "Iowa", "Kansas", "Kentucky", "Louisiana", "Maine", 
    "Maryland", "Massachusetts", "Michigan", "Minnesota", "Mississippi", "Missouri", "Montana", "Nebraska", 
    "Nevada", "New Hampshire", "New Jersey", "New Mexico", "New York", "North Carolina", "North Dakota", 
    "Ohio", "Oklahoma", "Oregon", "Pennsylvania", "Rhode Island", "South Carolina", "South Dakota", 
    "Tennessee", "Texas", "Utah", "Vermont", "Virginia", "Washington", "West Virginia", "Wisconsin", "Wyoming"
}

# --- 3. PROCESSING ENGINE ---

def clean_val(val):
    """Ensures a single value with no commas."""
    if not val or val == "-":
        return "-"
    # Take the first part before a comma and strip whitespace
    return str(val).split(",")[0].strip()

def process_note_record(text):
    """
    1. Filters start-of-sentence noise.
    2. Resolves hierarchy to single values.
    3. Calculates Final_Coordinates from the combined result.
    """
    cols = ['Areas', 'Validation', 'City', 'County', 'State', 'Country', 'Final_Coordinates']
    default_res = pd.Series(["-"] * 7, index=cols)
    
    if pd.isna(text) or text == '-':
        return default_res

    doc = nlp(str(text))
    # Filter: GPE/LOC entities NOT at the very start
    entities = [ent for ent in doc.ents if ent.label_ in ['GPE', 'LOC'] and ent.start_char > 0]
    
    if not entities:
        return default_res

    # Priority Logic: If multiple, take the last one (often the origin 'from Virginia')
    primary_name = entities[-1].text.strip()
    
    res = {k: "-" for k in cols}
    res['Areas'] = primary_name

    try:
        time.sleep(1.1)
        location = geolocator.geocode(primary_name, addressdetails=True, timeout=10)
        
        if location:
            res['Validation'] = "Yes"
            addr = location.raw.get('address', {})
            
            # Populate Hierarchy (Single values only)
            res['City'] = clean_val(addr.get('city') or addr.get('town') or addr.get('village') or addr.get('hamlet'))
            res['County'] = clean_val(addr.get('county'))
            res['State'] = clean_val(addr.get('state') or addr.get('province'))
            res['Country'] = clean_val(addr.get('country'))

            # Final Step: Combination-based Coordinates
            # Construct a string like "Charleston, Charleston County, South Carolina, USA"
            combined_query = ", ".join([res[k] for k in ['City', 'County', 'State', 'Country'] if res[k] != "-"])
            
            if combined_query:
                time.sleep(1.1)
                final_loc = geolocator.geocode(combined_query, timeout=10)
                if final_loc:
                    res['Final_Coordinates'] = f"{final_loc.latitude}, {final_loc.longitude}"
                else:
                    # Fallback to initial coordinates if combination fails
                    res['Final_Coordinates'] = f"{location.latitude}, {location.longitude}"
        else:
            res['Validation'] = "No"

    except Exception:
        res['Validation'] = "Error"

    return pd.Series([res[k] for k in cols], index=cols)

# --- 4. EXECUTION ---

def main():
    print(f"Reading {INPUT_FILE}...")
    try:
        df = pd.read_excel(INPUT_FILE)
    except Exception as e:
        print(f"Error: {e}")
        return

    print("Processing geographic context and hierarchy resolution...")
    processed_geo = df['Notes'].apply(process_note_record)
    
    # Merge and Save
    final_df = pd.concat([df, processed_geo], axis=1)
    
    print(f"Saving to {OUTPUT_FILE}...")
    final_df.to_excel(OUTPUT_FILE, index=False)
    print("Process Complete. Each record now contains singular geographic values.")

if __name__ == "__main__":
    main()