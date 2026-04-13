import pandas as pd
import spacy
import re

# Load spaCy small model
try:
    nlp = spacy.load("en_core_web_sm")
except OSError:
    from spacy.cli import download
    download("en_core_web_sm")
    nlp = spacy.load("en_core_web_sm")

# --- 1. Load Files ---
file_a_path = 'Consolidated_Directory.xlsx'
file_b_path = 'Book_of_Negroes_Copy.xlsx'

df_a = pd.read_excel(file_a_path)
df_b = pd.read_excel(file_b_path)

# Normalize reference file (fileB) column names
df_b.columns = [col.replace(" ", "_") for col in df_b.columns]

# --- 2. Create Matching Keys ---
df_a['Match_Name'] = (df_a['First_Name'].fillna('').astype(str).str.strip() + " " + 
                      df_a['Surname'].fillna('').astype(str).str.strip()).str.lower()
df_a['Match_Ship'] = df_a['Ship_Name'].fillna('').astype(str).str.strip().str.lower()

if 'Name' in df_b.columns:
    df_b['Match_Name'] = df_b['Name'].fillna('').astype(str).str.strip().str.lower()
df_b['Match_Ship'] = df_b['Ship_Name'].fillna('').astype(str).str.strip().str.lower()

# --- 3. Logic Functions ---

def extract_race_ethnicity_spacy(row):
    """Categorizes Race, Ethnicity, and Description using spaCy. Defaults to Black."""
    notes = str(row['Notes']) if pd.notna(row['Notes']) else ""
    
    # Default values as requested
    race, ethnicity, description = "Black", "African American", "Black"
    
    if not notes or notes.strip() == "":
        return pd.Series([race, ethnicity, description])

    doc = nlp(notes.lower())
    tokens = [token.text for token in doc]
    full_text = doc.text

    # Case: Quadroon
    if "quadroon" in tokens:
        race, ethnicity, description = "Quadroon", "Mixed Race", "Quadroon"
    
    # Case: Mulatto variants
    elif "mulatto" in tokens:
        ethnicity = "Mixed Race"
        description = "Mulatto"
        if "indian" in tokens and "half" in tokens:
            race = "Half Indian"
        else:
            race = "Mulatto"
            
    # Case: Complex Mixed Descriptors
    elif "indian" in tokens and "span" in tokens:
        if re.search(r'between.*indian.*span', full_text):
            race, ethnicity, description = "Indian/Spanish", "Mixed Race", "Mulatto"
        else:
            race, ethnicity, description = "Indian/Spanish", "Mixed Race", "Black"

    # Note: If none of the above are met, it remains the default (Black/African American/Black)
    return pd.Series([race, ethnicity, description])

def clean_text(text):
    if not text or text == "-": return "-"
    text = str(text)
    text = re.sub(r'\s+(?:left|served|employed|lived|born|formerly|was|property).*', '', text, flags=re.IGNORECASE)
    return text.strip(",.; ")

def extract_enslaver(notes):
    if pd.isna(notes) or not isinstance(notes, str) or notes.strip() == "": return '-'
    enslaver = '-'
    paren_matches = re.findall(r'\(([A-Z][A-Za-z\s\.]+)\)', notes)
    for match in paren_matches:
        if len(match.split()) >= 2:
            enslaver = match
            break
    if enslaver == '-':
        prop_match = re.search(r'Property of ([A-Z][a-z]+(?:\s[A-Z][a-z\.]+){1,2})', notes, re.IGNORECASE)
        if prop_match: enslaver = prop_match.group(1)
    return clean_text(enslaver)

def extract_geo(row):
    notes = str(row['Notes']) if pd.notna(row['Notes']) else ""
    existing_state = str(row['Origination_State']) if pd.notna(row['Origination_State']) else "-"
    city, county, state = "-", "-", existing_state

    if "norfolk co." in notes.lower(): county = "Norfolk"
    elif "nansemond co." in notes.lower(): county = "Nansemond"
    
    if not notes or notes.strip() == "": return pd.Series([city, county, state])
    
    if county == "-":
        county_regex = re.search(r'([A-Z][A-Za-z\s]+)\sCounty', notes)
        found_county_name = county_regex.group(0).strip() if county_regex else None
    else:
        found_county_name = None

    doc = nlp(re.sub(r'\(.*?\)', '', notes))
    entities = list(dict.fromkeys([ent.text for ent in doc.ents if ent.label_ in ["GPE", "LOC"]]))

    if len(entities) >= 3:
        city, state = entities[0], entities[2]
        if county == "-": county = entities[1]
    elif len(entities) == 2:
        if found_county_name and (found_county_name in entities[0]):
            state = entities[1]
            if county == "-": county = entities[0]
        elif "County" in entities[0]:
            state = entities[1]
            if county == "-": county = entities[0]
        else:
            city, state = entities[0], entities[1]
    elif len(entities) == 1:
        if found_county_name: 
            if county == "-": county = found_county_name
        else: 
            state = entities[0]
    
    if county == "-" and found_county_name: county = found_county_name
    city, county, state = clean_text(city), clean_text(county), clean_text(state)
    if state == "-": state = existing_state
    return pd.Series([city, county, state])

# --- 4. Processing ---

print("⚙️ Executing extraction with default 'Black' categorization...")

# 1. Citations
df_a['Primary_Source_1'] = "Book of Negroes, British Headquarters Papers PRO 30/55/100; Loyalist evacuation shipping lists (1782–1784)"

# 2. Extract Race/Ethnicity (spaCy-based with Black defaults)
race_data = df_a.apply(extract_race_ethnicity_spacy, axis=1)
df_a['Race'], df_a['Ethnicity'], df_a['Description'] = race_data[0], race_data[1], race_data[2]

# 3. Extract Enslaver and Geography
df_a['Enslaver'] = df_a['Notes'].apply(extract_enslaver)
geo_results = df_a.apply(extract_geo, axis=1)
df_a['Origination_City'], df_a['Origination_County'], df_a['Origination_State'] = geo_results[0], geo_results[1], geo_results[2]

# 4. Lookup Merge
lookup_cols = ['Match_Ship', 'Match_Name', 'Ref_Page', 'Origination_Port', 'Departure_Port', 'Departure_Date', 'Primary_Source_2']
df_b_match = df_b[lookup_cols].drop_duplicates(subset=['Match_Ship', 'Match_Name'])
df_final = df_a.merge(df_b_match, on=['Match_Ship', 'Match_Name'], how='left', suffixes=('', '_LKP'))

# 5. Defaults & Population
df_final['Origination_Port'] = df_final['Origination_Port_LKP'].fillna('New York')
df_final['Departure_Port'] = df_final['Departure_Port_LKP'].fillna('New York')
df_final['Departure_Date'] = df_final['Departure_Date_LKP'].fillna(1783)
df_final['Primary_Source_2'] = df_final['Primary_Source_2_LKP'].fillna(df_final.get('Primary_Source_2', '-'))
df_final['Ref_Page'] = df_final['Ref_Page_LKP'].fillna(df_final.get('Ref_Page', '-'))

# Cleanup
drop_cols = [c for c in df_final.columns if c.endswith('_LKP')] + ['Match_Name', 'Match_Ship']
df_final.drop(columns=drop_cols, inplace=True)

# --- 5. Column Reorganization ---

if 'Commander' in df_final.columns:
    ens_col = df_final.pop('Enslaver')
    idx = df_final.columns.tolist().index('Commander')
    df_final.insert(idx + 1, 'Enslaver', ens_col)

target_block = ['Origination_City', 'Origination_County', 'Origination_State', 'Origination_Port', 'Departure_Port', 'Departure_Date']
current_cols = df_final.columns.tolist()
insert_pos = current_cols.index('Description') + 1 if 'Description' in current_cols else len(current_cols)

for col in target_block:
    if col in current_cols: current_cols.remove(col)
for i, col in enumerate(target_block):
    current_cols.insert(insert_pos + i, col)

df_final = df_final[current_cols]

# Save Result
output_name = 'Consolidated_Directory_v3.xlsx'
df_final.to_excel(output_name, index=False)
print(f"✨ Success! Defaults set to Black. File saved as: {output_name}")