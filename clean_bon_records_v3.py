import pandas as pd
import spacy
import re
import time
import os
from openpyxl import load_workbook
from geopy.geocoders import Nominatim

# Load spaCy
try:
    nlp = spacy.load("en_core_web_md")
except OSError:
    from spacy.cli import download
    download("en_core_web_md")
    nlp = spacy.load("en_core_web_md")

geolocator = Nominatim(user_agent="historical_research_v17_final_rectified")

# --- 1. Configuration ---
file_path = 'Consolidated_Directory_v11_copy.xlsx'
file_c_path = 'US_Counties_Coordinates.xlsx' 

df_a = pd.read_excel(file_path)
df_c = pd.read_excel(file_c_path) 

# RECTIFICATION: Rename column as requested immediately
if 'Arrival_Port_City' in df_a.columns:
    df_a.rename(columns={'Arrival_Port_City': 'Arrival_Port'}, inplace=True)

LAT_COL = 'lat'
LON_COL = 'lng'

KNOWN_COUNTRIES = {"Germany", "England", "Britain", "France", "Jamaica", "Africa", "Canada", "United Kingdom"}

# INITIALIZATION: Create the new columns now so the Skip Logic doesn't crash
if 'Arrival_Port_Country' not in df_a.columns:
    df_a['Arrival_Port_Country'] = "Canada"
if 'Arrival_Coordinates' not in df_a.columns:
    df_a['Arrival_Coordinates'] = "-"
if 'Arrival_Port' not in df_a.columns:
    df_a['Arrival_Port'] = "-"

# --- 2. Rectified Ship Notes Parsing ---

def parse_ship_notes(row):
    notes = str(row.get('Ship_Notes', ""))
    # Maintain existing values if they are already meaningful
    port = str(row.get('Arrival_Port', "-"))
    country = str(row.get('Arrival_Port_Country', "Canada"))
    commander = str(row.get('Commander', "-"))
    
    if "bound for" in notes.lower():
        parts = re.split(r'bound for', notes, flags=re.IGNORECASE)
        dest_segment = parts[1].strip() if len(parts) > 1 else ""
        
        if dest_segment:
            doc = nlp(dest_segment)
            found_locs = [ent.text for ent in doc.ents if ent.label_ in ["GPE", "LOC"]]
            found_names = [ent.text for ent in doc.ents if ent.label_ == "PERSON"]
            
            # Identify Destination
            if found_locs:
                main_loc = found_locs[0]
                clean_loc = re.sub(r'.*?&\s*', '', main_loc).strip()
                
                if clean_loc in KNOWN_COUNTRIES:
                    country = clean_loc
                    port = "-"
                else:
                    port = clean_loc
                    country = next((c for c in KNOWN_COUNTRIES if c in dest_segment), "Canada")
            
            # Identify Commander (Priority to PERSON entity, then fallback)
            if found_names:
                commander = found_names[0]
            elif "," in dest_segment:
                potential_name = dest_segment.split(',')[0].strip()
                if potential_name not in found_locs:
                    commander = potential_name
            elif " " in dest_segment:
                words = dest_segment.split()
                # If there's a word after the identified port that isn't a location, it's likely the name
                remaining = [w for w in words if w not in str(port) and w not in KNOWN_COUNTRIES]
                if remaining:
                    potential = " ".join(remaining)
                    if potential[0].isupper():
                        commander = potential

    return pd.Series([port, country, commander])

# --- 3. Enhanced Extraction & Skip Logic ---

def process_entry(row):
    target_cols = ['Enslaver', 'Extracted_City', 'Extracted_County', 'Extracted_State', 'Extracted_Area', 'Country']
    
    # SKIP LOGIC: Check if work is already done
    dep_coord = str(row.get('Departure_Coordinates', "")).strip()
    arr_coord = str(row.get('Arrival_Coordinates', "")).strip()
    
    if dep_coord not in ["", "-", "nan"] or arr_coord not in ["", "-", "nan"]:
        return pd.Series([row.get(col, "-") for col in target_cols])

    notes = str(row.get('Notes', ""))
    enslaver = "-"
    enslaver_match = re.search(r'\((.*?)\)', notes)
    if enslaver_match:
        content = enslaver_match.group(1).strip()
        enslaver = "-" if "own bottom" in content.lower() else content
    
    clean_notes = re.sub(r'\(.*?\)', '', notes).strip()
    doc = nlp(clean_notes)
    city, county, state, area, country = "-", "-", "-", "-", "United States"
    
    gpes = [ent.text for ent in doc.ents if ent.label_ in ["GPE", "LOC"]]
    if len(gpes) >= 1: city = gpes[0]
    if len(gpes) >= 2: state = gpes[1]

    return pd.Series([enslaver, city, county, state, area, country])

# Process Data
print("Step 1: Parsing corrected Port and Commander fields...")
df_a[['Arrival_Port', 'Arrival_Port_Country', 'Commander']] = df_a.apply(parse_ship_notes, axis=1)
df_a[['Enslaver', 'Extracted_City', 'Extracted_County', 'Extracted_State', 'Extracted_Area', 'Country']] = df_a.apply(process_entry, axis=1)

# --- 4. Coordinate Caching Logic ---

def get_global_coordinate_map(df, df_lookup):
    # Filter for records where coordinates are missing/placeholder
    mask = ((df['Departure_Coordinates'].isna()) | (df['Departure_Coordinates'].isin(["", "-", "nan"]))) & \
           ((df['Arrival_Coordinates'].isna()) | (df['Arrival_Coordinates'].isin(["", "-", "nan"])))
    subset = df[mask]
    
    unique_dep = subset[['Extracted_City', 'Extracted_County', 'Extracted_State', 'Country']].drop_duplicates()
    unique_dep.columns = ['City', 'County', 'State', 'Country']

    unique_arr = subset[['Arrival_Port', 'Arrival_Port_Country']].drop_duplicates()
    unique_arr.columns = ['City', 'Country']
    unique_arr['County'], unique_arr['State'] = "-", "-"

    combined = pd.concat([unique_dep, unique_arr]).drop_duplicates()
    
    coord_map = {}
    total = len(combined)
    print(f"Step 2: Geocoding {total} unique locations...")

    for i, (_, row) in enumerate(combined.iterrows(), 1):
        city, county, state, country = row['City'], row['County'], row['State'], row['Country']
        loc_key = (city, county, state, country)
        
        geo_parts = [str(p) for p in [city, county, state, country] if p and p != "-" and pd.notna(p)]
        query = ", ".join(geo_parts)
        
        if not query or query in ["Canada", "United States"]:
            coord_map[loc_key] = "-"
            continue

        try:
            time.sleep(1.1)
            location = geolocator.geocode(query, timeout=10)
            coord_map[loc_key] = f"{location.latitude}, {location.longitude}" if location else "-"
        except:
            coord_map[loc_key] = "-"
            
    return coord_map

location_cache = get_global_coordinate_map(df_a, df_c)

# Final Mapping
def assign_coords(row, coord_type='dep'):
    dep_v = str(row.get('Departure_Coordinates', "")).strip()
    arr_v = str(row.get('Arrival_Coordinates', "")).strip()
    
    if dep_v not in ["", "-", "nan"] or arr_v not in ["", "-", "nan"]:
        return dep_v if coord_type == 'dep' else arr_v

    if coord_type == 'dep':
        return location_cache.get((row['Extracted_City'], row['Extracted_County'], row['Extracted_State'], row['Country']), "-")
    else:
        return location_cache.get((row['Arrival_Port'], "-", "-", row['Arrival_Port_Country']), "-")

df_a['Departure_Coordinates'] = df_a.apply(lambda r: assign_coords(r, 'dep'), axis=1)
df_a['Arrival_Coordinates'] = df_a.apply(lambda r: assign_coords(r, 'arr'), axis=1)

# --- 5. Export ---

column_order = [
    'ID', 'Book', 'First_Name', 'Surname', 'Ship_Name', 'Notes', 'Ship_Notes', 
    'Birthdate', 'Gender', 'Race', 'Ethnicity', 'Origin', 'Extracted_City', 
    'Extracted_County', 'Extracted_State', 'Extracted_Area', 'Country', 'State', 
    'County', 'Departure_Coordinates', 'Origination_Port', 'Origination_State', 
    'Departure_Port', 'Departure_Date', 'Arrival_Port', 'Arrival_Port_Country', 
    'Arrival_Coordinates', 'Father_FirstName', 'Father_Surname', 'Mother_FirstName', 
    'Mother_Surname', 'Ref_Page', 'Commander', 'Enslaver', 'Primary_Source_1', 'Primary_Source_2'
]

# Ensure all columns exist before subsetting
for col in column_order:
    if col not in df_a.columns:
        df_a[col] = "-"

df_final = df_a[column_order]

output_file = 'Consolidated_Directory_v13.xlsx'
with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
    df_final.to_excel(writer, index=False, sheet_name='Main')
    df_c.to_excel(writer, index=False, sheet_name='Sheet2')

print(f"✨ File saved as {output_file}")