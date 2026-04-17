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

geolocator = Nominatim(user_agent="maritime_historical_geocoder_v22")

INPUT_FILE = 'notes_sample.xlsx'
OUTPUT_FILE = 'Extracted_Geographic_Validation_v1.xlsx'
GEO_CACHE = {}

# --- 2. REFINEMENT UTILITIES ---

def scrub_maritime_noise(text):
    """Removes common maritime titles and person-related noise."""
    # Remove titles and suffixes
    noise = [r'\bMaster\b', r'\bLt\b', r'\bCapt\b', r'\bEsq\b', r'\bCol\b', r'\b&', r'\band\b']
    for pattern in noise:
        text = re.sub(pattern, '', text, flags=re.IGNORECASE)
    
    # Use NLP to identify Person entities within the specific phrase and remove them
    temp_doc = nlp(text)
    for ent in temp_doc.ents:
        if ent.label_ == "PERSON":
            text = text.replace(ent.text, "")
            
    # Clean up punctuation and extra whitespace
    text = re.sub(r'[,.]', '', text)
    return text.strip()

def get_geopy_data_cached(query, allow_fallback=True):
    """Priority: Canada -> US -> Global. Handles word-dropping fallback."""
    if not query or query == "-": return None
    if query in GEO_CACHE: return GEO_CACHE[query]
    
    try:
        time.sleep(1.1) 
        location = geolocator.geocode(query, addressdetails=True, timeout=10, country_codes='ca')
        if not location:
            location = geolocator.geocode(query, addressdetails=True, timeout=10, country_codes='us')
        if not location:
            location = geolocator.geocode(query, addressdetails=True, timeout=10)
            
        if location:
            GEO_CACHE[query] = location
            return location
            
        if allow_fallback and len(query.split()) > 1:
            return get_geopy_data_cached(" ".join(query.split()[:-1]), allow_fallback=False)
            
        GEO_CACHE[query] = None
        return None
    except:
        return None

# --- 3. PROCESSING ENGINE ---

def process_record(text):
    cols = ['Areas', 'Validation', 'City', 'Landmark', 'County', 'State', 'Country', 'Areas_for_coordinates', 'Final_Coordinates']
    res = {k: "-" for k in cols}
    found_data = {'City': [], 'Landmark': [], 'County': [], 'State': [], 'Country': []}
    
    if pd.isna(text) or text == '-':
        return pd.Series([res[k] for k in cols], index=cols)

    text_str = str(text)
    doc = nlp(text_str)
    raw_entities = []

    # 1. Advanced "Bound For" Extraction
    # Look for destinations between "bound for" and the end or a comma/name
    bound_pattern = re.search(r'bound for\s+(.*?)(?:,|$)', text_str, re.IGNORECASE)
    if bound_pattern:
        potential_dest = bound_pattern.group(1)
        # Handle cases like "Annapolis & St. John's"
        sub_parts = re.split(r'&|and', potential_dest)
        for p in sub_parts:
            cleaned = scrub_maritime_noise(p)
            if cleaned: raw_entities.append(cleaned)

    # 2. Global Entity extraction (Fallback for origins/areas before 'bound for')
    for ent in doc.ents:
        if ent.label_ in ['GPE', 'LOC'] and ent.text not in res['Areas']:
            cleaned = scrub_maritime_noise(ent.text)
            if cleaned: raw_entities.append(cleaned)

    unique_entities = list(dict.fromkeys([e for e in raw_entities if len(e) > 2]))
    if not unique_entities:
        return pd.Series([res[k] for k in cols], index=cols)

    res['Areas'] = ", ".join(unique_entities)
    val_status = []
    
    for area in unique_entities:
        loc = get_geopy_data_cached(area)
        if loc:
            val_status.append("Yes")
            addr = loc.raw.get('address', {})
            a_type = loc.raw.get('addresstype', '').lower()
            
            # Categorize the area
            if a_type in ['city', 'town', 'village', 'municipality']:
                v = area; found_data['City'].append(v)
            elif a_type in ['state', 'province']:
                v = area; found_data['State'].append(v)
            elif a_type == 'country':
                v = area; found_data['Country'].append(v)
            else:
                v = area; found_data['Landmark'].append(v)

            # Waterfall parents
            for key, geo_key in [('City', 'city'), ('County', 'county'), ('State', 'state'), ('Country', 'country')]:
                val = addr.get(geo_key) or addr.get('province') if geo_key == 'state' else addr.get(geo_key)
                if val and val not in found_data[key]:
                    found_data[key].append(val)
        else:
            val_status.append("No")

    # Map to columns
    for k in found_data:
        if found_data[k]: res[k] = ", ".join(list(dict.fromkeys(found_data[k])))
    res['Validation'] = ", ".join(val_status)

    # Matrix Generation
    test_queries = []
    all_hier = [v for k in ['Landmark', 'City', 'County', 'State', 'Country'] for v in found_data[k]]
    if all_hier: test_queries.append(", ".join(all_hier))
    for k in ['Landmark', 'City', 'State']:
        for item in found_data[k]:
            test_queries.append(item)
            if found_data['Country']: test_queries.append(f"{item}, {found_data['Country'][0]}")

    unique_queries = list(dict.fromkeys(test_queries))
    hits_area, hits_coord = [], []
    for q in unique_queries:
        loc = get_geopy_data_cached(q)
        if loc:
            hits_area.append(q)
            hits_coord.append(f"{loc.latitude}, {loc.longitude}")

    if hits_area:
        res['Areas_for_coordinates'] = " -- ".join(hits_area)
        res['Final_Coordinates'] = " -- ".join(hits_coord)

    return pd.Series([res[k] for k in cols], index=cols)

def main():
    df = pd.read_excel(INPUT_FILE)
    processed = df['Notes'].apply(process_record)
    pd.concat([df, processed], axis=1).to_excel(OUTPUT_FILE, index=False)
    print("Process Complete: Maritime destination scrubbing applied.")

if __name__ == "__main__":
    main()