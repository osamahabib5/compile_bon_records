import pandas as pd
import spacy
import re
import time
import os
from openpyxl import load_workbook
from openpyxl.worksheet.datavalidation import DataValidation
from openpyxl.utils import get_column_letter
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut

# Load spaCy
try:
    nlp = spacy.load("en_core_web_md")
except OSError:
    from spacy.cli import download
    download("en_core_web_md")
    nlp = spacy.load("en_core_web_md")

geolocator = Nominatim(user_agent="historical_research_v12_final_update")

# --- 1. Load Files ---
file_path = 'Consolidated_Directory_v10_copy.xlsx'
file_c_path = 'US_Counties_Coordinates.xlsx' 

df_a = pd.read_excel(file_path)
df_c = pd.read_excel(file_c_path) 

# Column mapping based on your US_Counties_Coordinates layout
LAT_COL = 'lat'
LON_COL = 'lng'

# Mapping for looking up full names in the Notes against 2-letter Sheet2 states
US_STATE_ABBR = {
    'Alabama': 'AL', 'Alaska': 'AK', 'Arizona': 'AZ', 'Arkansas': 'AR', 'California': 'CA',
    'Colorado': 'CO', 'Connecticut': 'CT', 'Delaware': 'DE', 'Florida': 'FL', 'Georgia': 'GA',
    'Hawaii': 'HI', 'Idaho': 'ID', 'Illinois': 'IL', 'Indiana': 'IN', 'Iowa': 'IA',
    'Kansas': 'KS', 'Kentucky': 'KY', 'Louisiana': 'LA', 'Maine': 'ME', 'Maryland': 'MD',
    'Massachusetts': 'MA', 'Michigan': 'MI', 'Minnesota': 'MN', 'Mississippi': 'MS',
    'Missouri': 'MO', 'Montana': 'MT', 'Nebraska': 'NE', 'Nevada': 'NV', 'New Hampshire': 'NH',
    'New Jersey': 'NJ', 'New Mexico': 'NM', 'New York': 'NY', 'North Carolina': 'NC',
    'North Dakota': 'ND', 'Ohio': 'OH', 'Oklahoma': 'OK', 'Oregon': 'OR', 'Pennsylvania': 'PA',
    'Rhode Island': 'RI', 'South Carolina': 'SC', 'South Dakota': 'SD', 'Tennessee': 'TN',
    'Texas': 'TX', 'Utah': 'UT', 'Vermont': 'VT', 'Virginia': 'VA', 'Washington': 'WA',
    'West Virginia': 'WV', 'Wisconsin': 'WI', 'Wyoming': 'WY'
}

# --- 2. Extraction Logic ---

def process_entry(row):
    # Ensure we return a 6-item Series to match the target columns
    target_cols = ['Enslaver', 'Extracted_City', 'Extracted_County', 'Extracted_State', 'Extracted_Area', 'Country']
    
    # SKIP logic: If coordinates already exist, return what is currently in the row
    if pd.notna(row.get('Departure_Coordinates')) and str(row.get('Departure_Coordinates')).strip() not in ["", "-"]:
        return pd.Series([row.get(col, "-") for col in target_cols])

    notes = str(row['Notes']) if pd.notna(row['Notes']) else ""
    
    # Enslaver extraction
    enslaver = "-"
    enslaver_match = re.search(r'\((.*?)\)', notes)
    if enslaver_match:
        content = enslaver_match.group(1).strip()
        enslaver = "-" if "own bottom" in content.lower() else content
    
    clean_notes = re.sub(r'\(.*?\)', '', notes).strip()
    doc = nlp(clean_notes)
    
    # Geography placeholders
    city, county, state, area, country = "-", "-", "-", "-", "United States"
    
    # Regex Keywords (Parish and County)
    parish_match = re.search(r'parish of\s+([A-Z][a-z]+(?:\s[A-Z][a-z]+)?)', clean_notes, re.I)
    county_kw_match = re.search(r'([A-Z][a-z]+(?:\s[A-Z][a-z]+)?)\s+County', clean_notes)
    
    # International & Special Case Logic
    note_l = clean_notes.lower()
    if "madagascar" in note_l:
        country = "Madagascar"
    elif "jamaica" in note_l:
        country = "Jamaica"
    elif "london" in note_l or "england" in note_l:
        country = "United Kingdom"
        city = "London"

    # Area Extraction (e.g., Long Island)
    if "long island" in note_l:
        area = "Long Island"
        state = "New York"

    # Base assignment from spaCy entities
    gpes = [ent.text for ent in doc.ents if ent.label_ in ["GPE", "LOC"]]
    if city == "-" and len(gpes) >= 1: city = gpes[0]
    if state == "-" and len(gpes) >= 2: state = gpes[1]

    # County overrides (Regex priority)
    if parish_match:
        county = parish_match.group(1).strip()
    elif county_kw_match:
        county = county_kw_match.group(1).strip()

    # Determine Country correctly based on extracted State/County
    us_state_abbrs = set(df_c['State'].unique())
    state_to_check = US_STATE_ABBR.get(state, state)
    if state_to_check in us_state_abbrs or (county != "-" and county in set(df_c['County'].unique())):
        country = "United States"

    return pd.Series([enslaver, city, county, state, area, country])

# Initialize columns if they are missing
for col in ['Enslaver', 'Extracted_City', 'Extracted_County', 'Extracted_State', 'Extracted_Area', 'Country']:
    if col not in df_a.columns:
        df_a[col] = "-"

print("Step 1: Extracting text entities and updating geography...")
df_a[['Enslaver', 'Extracted_City', 'Extracted_County', 'Extracted_State', 'Extracted_Area', 'Country']] = df_a.apply(process_entry, axis=1)

# --- 3. Optimized Coordinate Caching ---

def get_coordinate_map(df, df_lookup):
    # Only process rows where coordinates are currently empty or missing
    mask = (df['Departure_Coordinates'].isna()) | (df['Departure_Coordinates'].isin(["", "-"]))
    unique_locs = df[mask][['Extracted_City', 'Extracted_County', 'Extracted_State', 'Country']].drop_duplicates()
    
    coord_map = {}
    total_unique = len(unique_locs)
    print(f"Step 2: Geocoding {total_unique} unique missing locations...")

    for i, (_, row) in enumerate(unique_locs.iterrows(), 1):
        city, county, state, country = row['Extracted_City'], row['Extracted_County'], row['Extracted_State'], row['Country']
        loc_key = (city, county, state, country)
        
        if all(x == "-" for x in [city, county, state]):
            coord_map[loc_key] = "-"
            continue

        # A. Sheet2 Lookup (US Locations)
        if country == "United States":
            state_abbr = US_STATE_ABBR.get(state, state)
            match = df_lookup[(df_lookup['State'].astype(str) == str(state_abbr)) & 
                              (df_lookup['County'].astype(str) == str(county))]
            if not match.empty:
                coord_map[loc_key] = f"{match.iloc[0][LAT_COL]}, {match.iloc[0][LON_COL]}"
                continue

        # B. Geopy Fallback (International or unmatched US)
        geo_parts = [str(p) for p in [city, county, state, country] if p and p != "-"]
        query = ", ".join(geo_parts)
        try:
            if i % 10 == 0: print(f"  > API Progress: {i}/{total_unique} unique locations...")
            time.sleep(1.1)
            location = geolocator.geocode(query, timeout=10)
            coord_map[loc_key] = f"{location.latitude}, {location.longitude}" if location else "-"
        except:
            coord_map[loc_key] = "-"
            
    return coord_map

location_cache = get_coordinate_map(df_a, df_c)

# Final Coordinate assignment
df_a['Departure_Coordinates'] = df_a.apply(
    lambda r: r['Departure_Coordinates'] if pd.notna(r['Departure_Coordinates']) and str(r['Departure_Coordinates']).strip() not in ["", "-"]
    else location_cache.get((r['Extracted_City'], r['Extracted_County'], r['Extracted_State'], r['Country']), "-"),
    axis=1
)

# --- 4. Final Formatting & Reorganizing ---

if 'Description' in df_a.columns:
    df_a.rename(columns={'Description': 'Origin'}, inplace=True)

# Placeholders for Excel dropdown UI
df_a['State'], df_a['County'] = "", ""

fixed_start = ['Ship_Name', 'Notes', 'Ship_Notes', 'First_Name', 'Surname', 'Birthdate']
geo_block = [
    'Origin', 'Extracted_City', 'Extracted_County', 'Extracted_State', 
    'Extracted_Area', 'Country', 'State', 'County', 'Departure_Coordinates'
]
remaining = [c for c in df_a.columns if c not in fixed_start + geo_block and 'Match_' not in c]

df_final = df_a[fixed_start + remaining + geo_block]

# --- 5. Export ---
output_file = 'Consolidated_Directory_v11.xlsx'
with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
    df_final.to_excel(writer, index=False, sheet_name='Main')
    df_c.to_excel(writer, index=False, sheet_name='Sheet2')

print(f"✨ Script finished successfully. Saved to: {output_file}")