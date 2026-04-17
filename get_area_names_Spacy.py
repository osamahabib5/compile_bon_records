import pandas as pd
import spacy
from geopy.geocoders import Nominatim
import time

# --- 1. SETUP ---
try:
    nlp = spacy.load("en_core_web_md")
except OSError:
    from spacy.cli import download
    download("en_core_web_md")
    nlp = spacy.load("en_core_web_md")

# Initialize Geocoder
geolocator = Nominatim(user_agent="historical_matrix_geocoder_v17")

INPUT_FILE = 'notes_sample.xlsx'
OUTPUT_FILE = 'Extracted_Geographic_Validation.xlsx'

# Global cache to prevent redundant API calls
GEO_CACHE = {}

# List of US States for quick identification
US_STATES = {
    "Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado", "Connecticut", 
    "Delaware", "Florida", "Georgia", "Hawaii", "Idaho", "Illinois", "Indiana", "Iowa", 
    "Kansas", "Kentucky", "Louisiana", "Maine", "Maryland", "Massachusetts", "Michigan", 
    "Minnesota", "Mississippi", "Missouri", "Montana", "Nebraska", "Nevada", "New Hampshire", 
    "New Jersey", "New Mexico", "New York", "North Carolina", "North Dakota", "Ohio", 
    "Oklahoma", "Oregon", "Pennsylvania", "Rhode Island", "South Carolina", "South Dakota", 
    "Tennessee", "Texas", "Utah", "Vermont", "Virginia", "Washington", "West Virginia", 
    "Wisconsin", "Wyoming", "District of Columbia"
}

# --- 2. UTILITY FUNCTIONS ---

def clean_val(val):
    """Ensures a single value with no commas."""
    if not val or val == "-":
        return "-"
    return str(val).split(",")[0].strip()

def get_geopy_data_cached(query):
    """
    Fetches location data with rate limiting and 
    checks global cache to avoid redundant API calls.
    """
    if not query or query == "-":
        return None
    
    # Check if we already looked this up in this session
    if query in GEO_CACHE:
        return GEO_CACHE[query]
    
    try:
        time.sleep(1.1) # Respect Nominatim usage policy
        location = geolocator.geocode(query, addressdetails=True, timeout=10)
        GEO_CACHE[query] = location # Store in cache
        return location
    except:
        return None

# --- 3. PROCESSING ENGINE ---

def process_record(text):
    """
    STAGES:
    1. Extraction & Areas Population.
    2. US State Detection (Sets Country to US if state found).
    3. Exhaustive Classification (using Cache).
    4. Matrix Generation with ' - ' formatting.
    """
    cols = ['Areas', 'Validation', 'City', 'Landmark', 'County', 'State', 'Country', 'Areas_for_coordinates', 'Final_Coordinates']
    res = {k: "-" for k in cols}
    
    if pd.isna(text) or text == '-':
        return pd.Series([res[k] for k in cols], index=cols)

    doc = nlp(str(text))
    # Extract entities, skipping index 0 (names)
    entities = [ent.text.strip() for ent in doc.ents if ent.label_ in ['GPE', 'LOC'] and ent.start_char > 0]
    
    if not entities:
        return pd.Series([res[k] for k in cols], index=cols)

    res['Areas'] = ", ".join(list(dict.fromkeys(entities)))
    unique_entities = list(dict.fromkeys(entities))

    # --- STAGE 1: US STATE & COUNTRY CHECK ---
    found_us_state = False
    for area in unique_entities:
        if area in US_STATES:
            res['State'] = area
            res['Country'] = "United States"
            found_us_state = True
            break

    # --- STAGE 2: CLASSIFICATION ---
    val_status = []
    metadata_list = []
    
    for area in unique_entities:
        # If we already identified it as a US State, we still "validate" it
        loc = get_geopy_data_cached(area)
        if loc:
            val_status.append("Yes")
            raw = loc.raw
            addr = raw.get('address', {})
            a_type = raw.get('addresstype', '').lower()
            metadata_list.append(addr)

            # Classification assignment
            if a_type in ['city', 'town', 'village', 'hamlet', 'municipality', 'suburb']:
                if res['City'] == "-": res['City'] = clean_val(area)
            elif a_type in ['county', 'district', 'county_district']:
                if res['County'] == "-": res['County'] = clean_val(area)
            elif a_type in ['state', 'province', 'state_district']:
                if res['State'] == "-": res['State'] = clean_val(area)
            elif a_type == 'country':
                if res['Country'] == "-": res['Country'] = clean_val(area)
            else:
                if res['Landmark'] == "-": res['Landmark'] = clean_val(area)
        else:
            val_status.append("No")

    # Waterfall fill gaps
    for addr in metadata_list:
        if res['City'] == "-":
            v = addr.get('city') or addr.get('town') or addr.get('village')
            if v: res['City'] = clean_val(v)
        if res['County'] == "-":
            v = addr.get('county')
            if v: res['County'] = clean_val(v)
        if res['State'] == "-":
            v = addr.get('state') or addr.get('province')
            if v: res['State'] = clean_val(v)
        if res['Country'] == "-":
            v = addr.get('country')
            if v: res['Country'] = clean_val(v)

    res['Validation'] = ", ".join(val_status)

    # --- STAGE 3: MATRIX GENERATION ---
    test_queries = []
    hierarchy = [res['Landmark'], res['City'], res['County'], res['State'], res['Country']]
    
    # Combination 1: Full Hierarchy (comma-separated)
    full_str = ", ".join([v for v in hierarchy if v != "-"])
    if full_str: test_queries.append(full_str)
    
    # Combination 2: Logical Pairs
    if res['City'] != "-" and res['State'] != "-":
        test_queries.append(f"{res['City']}, {res['State']}")
    if res['City'] != "-" and res['Country'] != "-":
        test_queries.append(f"{res['City']}, {res['Country']}")
    
    # Combination 3: Individual parts
    for v in hierarchy:
        if v != "-": test_queries.append(v)
        
    unique_queries = []
    for q in test_queries:
        if q not in unique_queries: unique_queries.append(q)

    hits_area = []
    hits_coord = []
    
    for q in unique_queries:
        loc = get_geopy_data_cached(q) # Uses cache
        if loc:
            hits_area.append(q)
            hits_coord.append(f"{loc.latitude}, {loc.longitude}")

    # Formatting with " - " separator
    if hits_area:
        res['Areas_for_coordinates'] = " - ".join(hits_area)
        res['Final_Coordinates'] = " - ".join(hits_coord)

    return pd.Series([res[k] for k in cols], index=cols)

# --- 4. MAIN EXECUTION ---

def main():
    print(f"Reading {INPUT_FILE}...")
    try:
        df = pd.read_excel(INPUT_FILE)
    except Exception as e:
        print(f"Error: {e}")
        return

    print("Processing records with US State detection and Global Caching...")
    start_time = time.time()
    
    processed_df = df['Notes'].apply(process_record)
    final_df = pd.concat([df, processed_df], axis=1)

    print(f"Saving to {OUTPUT_FILE}...")
    final_df.to_excel(OUTPUT_FILE, index=False)
    
    end_time = time.time()
    print(f"Process Complete in {round(end_time - start_time, 2)} seconds.")
    print(f"Total Unique API queries handled: {len(GEO_CACHE)}")

if __name__ == "__main__":
    main()