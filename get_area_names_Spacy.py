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
geolocator = Nominatim(user_agent="historical_multi_entity_geocoder_v19")

INPUT_FILE = 'notes_sample.xlsx'
OUTPUT_FILE = 'Extracted_Geographic_Validation.xlsx'

# Global cache to prevent redundant API calls
GEO_CACHE = {}

# --- 2. UTILITY FUNCTIONS ---

def clean_val(val):
    """Ensures a single value with no internal commas."""
    if not val or val == "-":
        return None
    return str(val).split(",")[0].strip()

def get_geopy_data_cached(query, priority_us=False):
    """
    Fetches location data with rate limiting and caching.
    Tries US-specific search first if requested.
    """
    cache_key = f"{query}_US" if priority_us else f"{query}_Global"
    
    if cache_key in GEO_CACHE:
        return GEO_CACHE[cache_key]
    
    try:
        time.sleep(1.1) 
        if priority_us:
            location = geolocator.geocode(query, addressdetails=True, timeout=10, country_codes='us')
            if location:
                GEO_CACHE[cache_key] = location
                return location
        
        location = geolocator.geocode(query, addressdetails=True, timeout=10)
        GEO_CACHE[cache_key] = location
        return location
    except:
        return None

# --- 3. PROCESSING ENGINE ---

def process_record(text):
    """
    1. Extraction of all GPE/LOC entities.
    2. US-First exhaustive classification for every entity.
    3. Multi-value aggregation (comma-separated within columns).
    4. Matrix generation for coordinate sets.
    """
    cols = ['Areas', 'Validation', 'City', 'Landmark', 'County', 'State', 'Country', 'Areas_for_coordinates', 'Final_Coordinates']
    res = {k: "-" for k in cols}
    
    # Internal lists to hold multiple values per category
    found_data = {
        'City': [],
        'Landmark': [],
        'County': [],
        'State': [],
        'Country': []
    }
    
    if pd.isna(text) or text == '-':
        return pd.Series([res[k] for k in cols], index=cols)

    doc = nlp(str(text))
    entities = [ent.text.strip() for ent in doc.ents if ent.label_ in ['GPE', 'LOC'] and ent.start_char > 0]
    
    if not entities:
        return pd.Series([res[k] for k in cols], index=cols)

    res['Areas'] = ", ".join(list(dict.fromkeys(entities)))
    unique_entities = list(dict.fromkeys(entities))

    val_status = []
    
    # --- STAGE 1: EXHAUSTIVE MULTI-VALUE CLASSIFICATION ---
    for area in unique_entities:
        # Check US First
        loc = get_geopy_data_cached(area, priority_us=True)
        
        if loc:
            val_status.append("Yes")
            addr = loc.raw.get('address', {})
            a_type = loc.raw.get('addresstype', '').lower()
            
            # Identify high-level country context
            if addr.get('country_code') == 'us' and "United States" not in found_data['Country']:
                found_data['Country'].append("United States")

            # Categorize the specific entity extracted from 'Areas'
            if a_type in ['city', 'town', 'village', 'hamlet', 'municipality', 'suburb']:
                val = clean_val(area)
                if val and val not in found_data['City']: found_data['City'].append(val)
            elif a_type in ['county', 'district', 'county_district']:
                val = clean_val(area)
                if val and val not in found_data['County']: found_data['County'].append(val)
            elif a_type in ['state', 'province', 'state_district']:
                val = clean_val(area)
                if val and val not in found_data['State']: found_data['State'].append(val)
            elif a_type == 'country':
                val = clean_val(area)
                if val and val not in found_data['Country']: found_data['Country'].append(val)
            else:
                val = clean_val(area)
                if val and val not in found_data['Landmark']: found_data['Landmark'].append(val)

            # Waterfall fill: Extract parent hierarchy from the geocode result
            g_city = clean_val(addr.get('city') or addr.get('town') or addr.get('village'))
            g_county = clean_val(addr.get('county'))
            g_state = clean_val(addr.get('state') or addr.get('province'))
            g_country = clean_val(addr.get('country'))

            if g_city and g_city not in found_data['City']: found_data['City'].append(g_city)
            if g_county and g_county not in found_data['County']: found_data['County'].append(g_county)
            if g_state and g_state not in found_data['State']: found_data['State'].append(g_state)
            if g_country and g_country not in found_data['Country']: found_data['Country'].append(g_country)
        else:
            val_status.append("No")

    # Finalize administrative columns (join multi-values with comma)
    for key in found_data:
        if found_data[key]:
            res[key] = ", ".join(found_data[key])

    res['Validation'] = ", ".join(val_status)

    # --- STAGE 2: MATRIX GENERATION ---
    test_queries = []
    
    # 1. Full Combined hierarchy (all identified parts)
    all_parts = []
    for k in ['Landmark', 'City', 'County', 'State', 'Country']:
        if found_data[k]: all_parts.extend(found_data[k])
    
    if all_parts:
        test_queries.append(", ".join(all_parts))
    
    # 2. Add individual entities for verification
    for k in ['Landmark', 'City', 'County', 'State', 'Country']:
        for item in found_data[k]:
            # Try item alone
            test_queries.append(item)
            # Try item + first found country
            if found_data['Country']:
                test_queries.append(f"{item}, {found_data['Country'][0]}")

    unique_queries = []
    for q in test_queries:
        if q not in unique_queries: unique_queries.append(q)

    hits_area = []
    hits_coord = []
    
    for q in unique_queries:
        is_us = ("United States" in res['Country'])
        loc = get_geopy_data_cached(q, priority_us=is_us)
        if loc:
            hits_area.append(q)
            hits_coord.append(f"{loc.latitude}, {loc.longitude}")

    if hits_area:
        res['Areas_for_coordinates'] = " -- ".join(hits_area)
        res['Final_Coordinates'] = " -- ".join(hits_coord)

    return pd.Series([res[k] for k in cols], index=cols)

# --- 4. MAIN EXECUTION ---

def main():
    print(f"Reading {INPUT_FILE}...")
    try:
        df = pd.read_excel(INPUT_FILE)
    except Exception as e:
        print(f"Error: {e}")
        return

    print("Processing multi-value geographic entities with US-Priority...")
    start_time = time.time()
    
    processed_df = df['Notes'].apply(process_record)
    final_df = pd.concat([df, processed_df], axis=1)

    print(f"Saving multi-value results to {OUTPUT_FILE}...")
    final_df.to_excel(OUTPUT_FILE, index=False)
    
    end_time = time.time()
    print(f"Finished in {round(end_time - start_time, 2)} seconds.")

if __name__ == "__main__":
    main()