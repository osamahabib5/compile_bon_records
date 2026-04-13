import pandas as pd
import spacy
import re
import time
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

geolocator = Nominatim(user_agent="historical_research_v10_caching")

# --- 1. Load Files ---
file_a_path = 'Consolidated_Directory.xlsx'
file_b_path = 'Book_of_Negroes_Copy.xlsx'
file_c_path = 'US_Counties_Coordinates.xlsx' 

df_a = pd.read_excel(file_a_path)
df_b = pd.read_excel(file_b_path)
df_c = pd.read_excel(file_c_path)

df_b.columns = [col.replace(" ", "_") for col in df_b.columns]
VALID_COUNTRIES = {"United Kingdom", "Jamaica", "Madagascar", "Africa", "England", "France", "Spain", "Canada"}

# --- 2. Extraction Logic ---

def process_entry(row):
    notes = str(row['Notes']) if pd.notna(row['Notes']) else ""
    
    # 1. Extract Enslaver
    enslaver = "-"
    enslaver_match = re.search(r'\((.*?)\)', notes)
    if enslaver_match:
        content = enslaver_match.group(1).strip()
        enslaver = "-" if "own bottom" in content.lower() else content
    
    clean_notes = re.sub(r'\(.*?\)', '', notes).strip()
    doc = nlp(clean_notes)
    
    # 2. Identify Geography
    city, county, state, country = "-", "-", "-", "United States"
    
    # Keyword Search for County (e.g., "Princess Ann County")
    county_kw_match = re.search(r'([A-Z][a-z]+(?:\s[A-Z][a-z]+)?)\s+County', clean_notes)
    parish_match = re.search(r'parish of\s+([A-Z][a-z]+(?:\s[A-Z][a-z]+)?)', clean_notes, re.I)

    gpes = [ent.text for ent in doc.ents if ent.label_ in ["GPE", "LOC"]]
    
    for gpe in gpes:
        if gpe in VALID_COUNTRIES:
            country = gpe
            break
    
    if len(gpes) >= 3: city, county, state = gpes[0], gpes[1], gpes[2]
    elif len(gpes) == 2: city, state = gpes[0], gpes[1]
    elif len(gpes) == 1: state = gpes[0]
    
    if county_kw_match:
        county = county_kw_match.group(1).strip()
    elif parish_match:
        county = parish_match.group(1).strip()

    us_states = set(df_c['State'].unique())
    if state in us_states or (county != "-" and county in set(df_c['County'].unique())):
        country = "United States"

    return pd.Series([enslaver, city, county, state, country])

# Apply basic text extraction first
print("Step 1: Extracting text entities from Notes...")
df_a[['Enslaver', 'Extracted_City', 'Extracted_County', 'Extracted_State', 'Country']] = df_a.apply(process_entry, axis=1)

# --- 3. Optimized Coordinate Logic (The Cache) ---

def get_coordinate_map(df, df_lookup):
    """
    Creates a mapping of unique location combinations to coordinates.
    """
    # Create a unique list of location combinations found in the data
    unique_locs = df[['Extracted_City', 'Extracted_County', 'Extracted_State', 'Country']].drop_duplicates()
    coord_map = {}
    
    total_unique = len(unique_locs)
    print(f"Step 2: Geocoding {total_unique} unique locations (Optimized)...")

    for i, (_, row) in enumerate(unique_locs.iterrows(), 1):
        city, county, state, country = row['Extracted_City'], row['Extracted_County'], row['Extracted_State'], row['Country']
        loc_key = (city, county, state, country)
        
        # skip if no data at all
        if all(x == "-" for x in [city, county, state]):
            coord_map[loc_key] = "-"
            continue

        # 1. Try Sheet2 (US Only)
        if country == "United States":
            match = df_lookup[(df_lookup['State'].astype(str) == str(state)) & 
                              (df_lookup['County'].astype(str) == str(county))]
            if not match.empty:
                coord_map[loc_key] = f"{match.iloc[0]['Latitude']}, {match.iloc[0]['Longitude']}"
                continue

        # 2. Try Geopy API
        geo_parts = [str(p) for p in [city, county, state, country] if p and p != "-"]
        query = ", ".join(geo_parts)
        try:
            if i % 5 == 0: print(f"  > API Progress: {i}/{total_unique} unique locations...")
            time.sleep(1.1) # Respect Nominatim 1 request/sec policy
            location = geolocator.geocode(query, timeout=10)
            coord_map[loc_key] = f"{location.latitude}, {location.longitude}" if location else "-"
        except:
            coord_map[loc_key] = "-"
            
    return coord_map

# Generate map and apply it
location_cache = get_coordinate_map(df_a, df_c)
df_a['Departure_Coordinates'] = df_a.apply(lambda x: location_cache.get(
    (x['Extracted_City'], x['Extracted_County'], x['Extracted_State'], x['Country']), "-"
), axis=1)

# --- 4. Cleaning & Merging ---
if 'Description' in df_a.columns:
    df_a.rename(columns={'Description': 'Origin'}, inplace=True)

if 'Age' in df_a.columns:
    df_a['Birthdate'] = df_a['Age'].apply(lambda x: 1783 - int(float(x)) if pd.notna(x) and str(x).replace('.','').isdigit() else "-")
    df_a.drop(columns=['Age'], inplace=True)

df_a['State'], df_a['County'] = "", ""

# Merge with File B logic
df_a['Match_Name'] = (df_a['First_Name'].fillna('').astype(str).str.strip() + " " + 
                      df_a['Surname'].fillna('').astype(str).str.strip()).str.lower()
df_a['Match_Ship'] = df_a['Ship_Name'].fillna('').astype(str).str.strip().str.lower()

if 'Name' in df_b.columns:
    df_b['Match_Name'] = df_b['Name'].fillna('').astype(str).str.strip().str.lower()
if 'Ship_Name' in df_b.columns:
    df_b['Match_Ship'] = df_b['Ship_Name'].fillna('').astype(str).str.strip().str.lower()

target_cols = ['Ref_Page', 'Origination_Port', 'Departure_Port', 'Departure_Date', 'Arrival_Port_City', 'Ship_Notes']
lookup_cols = ['Match_Ship', 'Match_Name'] + [c for c in target_cols if c in df_b.columns]
df_b_match = df_b[lookup_cols].drop_duplicates(subset=['Match_Ship', 'Match_Name'])
df_final = df_a.merge(df_b_match, on=['Match_Ship', 'Match_Name'], how='left', suffixes=('', '_LKP'))

# Reorganize Columns
fixed_start = ['Ship_Name', 'Notes', 'Ship_Notes', 'First_Name', 'Surname', 'Birthdate']
geo_block = ['Origin', 'Extracted_City', 'Extracted_County', 'Extracted_State', 'Country', 'State', 'County', 'Departure_Coordinates']
all_cols = df_final.columns.tolist()
remaining = [c for c in all_cols if c not in fixed_start + geo_block and not c.endswith('_LKP') and 'Match_' not in c]
df_final = df_final[fixed_start + remaining + geo_block]

# --- 5. Export & Dropdowns ---
output_file = 'Consolidated_Directory_v10.xlsx'
df_c = df_c.sort_values(['State', 'County'])

with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
    df_final.to_excel(writer, index=False, sheet_name='Main')
    df_c.to_excel(writer, index=False, sheet_name='Sheet2')

wb = load_workbook(output_file)
ws_main = wb['Main']
header = {cell.value: get_column_letter(cell.column) for cell in ws_main[1]}

unique_states = sorted(df_c['State'].unique().astype(str).tolist())
for i, s in enumerate(unique_states, 1):
    wb['Sheet2'].cell(row=i, column=10).value = s 

dv_state = DataValidation(type="list", formula1=f"Sheet2!$J$1:$J${len(unique_states)}", allow_blank=True)
ws_main.add_data_validation(dv_state)

for r in range(2, ws_main.max_row + 1):
    dv_state.add(ws_main[f"{header['State']}{r}"])
    st_cell = f"{header['State']}{r}"
    formula_co = f'OFFSET(Sheet2!$A$1, MATCH({st_cell}, Sheet2!$B:$B, 0)-1, 0, COUNTIF(Sheet2!$B:$B, {st_cell}), 1)'
    dv_county = DataValidation(type="list", formula1=formula_co, allow_blank=True)
    ws_main.add_data_validation(dv_county)
    dv_county.add(ws_main[f"{header['County']}{r}"])

wb.save(output_file)
print(f"✨ Process complete. Optimized output saved to: {output_file}")