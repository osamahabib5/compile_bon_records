import pandas as pd
import spacy
import openpyxl
from spacy.matcher import Matcher
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
import time
import re

# --- 1. SETUP ---
try:
    nlp = spacy.load("en_core_web_md")
except OSError:
    from spacy.cli import download
    download("en_core_web_md")
    nlp = spacy.load("en_core_web_md")

geolocator = Nominatim(user_agent="maritime_fuzzy_resolver_v32")
geocode_service = RateLimiter(geolocator.geocode, min_delay_seconds=1.1)

INPUT_FILE = 'Extracted_Geographic_Validation_v4.xlsx'
OUTPUT_FILE = 'Extracted_Geographic_Validation_v5.xlsx'

# Words to be stripped from area names
NOISE_WORDS = {"Esq", "KAD", "A.L", "mo", "Master", "Lt", "Capt", "Jnr", "Snr"}
GEO_CACHE = {}

# --- 2. FUZZY MATCHER CONFIGURATION ---
# We use spaCy's Matcher to identify misspelled versions of key locations
matcher = Matcher(nlp.vocab)

# Pattern for Charlestown (allowing for small misspellings or variants)
matcher.add("CHARLESTON_FIX", [[{"TEXT": {"FUZZY": "Charlestown"}, "OP": "+"}]])
# Pattern for Wando River (fixing common misspellings like Wanda)
matcher.add("WANDO_FIX", [[{"TEXT": {"FUZZY": "Wanda"}, "OP": "+"}, {"LOWER": "river"}]])

# --- 3. UTILITY FUNCTIONS ---

def is_cell_yellow(cell):
    """Detects #FFFF00 yellow highlights."""
    if not cell or not cell.fill or not cell.fill.start_color:
        return False
    color_val = str(cell.fill.start_color.index).upper()
    return color_val in ["FFFF00", "FFFFFF00", "00FFFF00", "FFFF0000"]

def apply_spacy_fuzzy_logic(text):
    """
    Uses spaCy Matcher to perform fuzzy replacements for specific 
    historical location errors like Charlestown or Wanda River.
    """
    if not text or text == "-":
        return text
        
    doc = nlp(text)
    matches = matcher(doc)
    
    # Process matches in reverse to avoid index shifting during replacement
    new_text = text
    for match_id, start, end in reversed(matches):
        string_id = nlp.vocab.strings[match_id]
        span = doc[start:end]
        
        if string_id == "CHARLESTON_FIX":
            new_text = new_text.replace(span.text, "Charleston, South Carolina")
        elif string_id == "WANDO_FIX":
            new_text = new_text.replace(span.text, "Wando River")
            
    return new_text

def clean_area_entry(area_text):
    """
    Strips noise words and abbreviations while maintaining 
    connectivity (e.g., 'Isle of Wight, Virginia').
    """
    if not area_text or area_text == "-":
        return None
        
    # Apply spaCy fuzzy logic first
    area_text = apply_spacy_fuzzy_logic(area_text)
    
    # Filter out noise words
    parts = [p.strip() for p in area_text.split(',')]
    cleaned_parts = []
    for p in parts:
        words = p.split()
        filtered = [w for w in words if w.strip('.,') not in NOISE_WORDS and len(w.strip('.,')) > 1]
        new_p = " ".join(filtered)
        if new_p:
            cleaned_parts.append(new_p)
            
    return ", ".join(cleaned_parts) if cleaned_parts else None

def get_geopy_data_cached(query):
    if query in GEO_CACHE:
        return GEO_CACHE[query]
    try:
        location = geocode_service(query, addressdetails=True, timeout=10)
        GEO_CACHE[query] = location
        return location
    except:
        return None

# --- 4. PROCESSING ENGINE ---

def process_row_intelligence(row, is_highlighted):
    """
    Determines if row needs update and enforces 'Connected' hierarchy.
    """
    raw_areas = str(row.get('Areas', "-"))
    
    # Criteria: Highlights, Charlestown mentions, Wanda mentions, or Multi-areas
    trigger_words = ["Charlestown", "Wanda", "River"]
    needs_processing = (is_highlighted or 
                        any(word.lower() in raw_areas.lower() for word in trigger_words) or 
                        any(noise in raw_areas for noise in NOISE_WORDS) or
                        "," in raw_areas)

    if not needs_processing or raw_areas == "-":
        return row

    cols_to_update = ['Validation', 'City', 'Landmark', 'County', 'State', 'Country', 'Areas_for_coordinates', 'Final_Coordinates']
    found_data = {'City': [], 'Landmark': [], 'County': [], 'State': [], 'Country': []}
    
    cleaned_query = clean_area_entry(raw_areas)
    if not cleaned_query: return row

    # Search as a 'Connected' unit to lock in the hierarchy (e.g. Raritan, New Jersey)
    loc = get_geopy_data_cached(cleaned_query)
    
    if loc:
        row['Validation'] = "Yes"
        addr = loc.raw.get('address', {})
        a_type = loc.raw.get('addresstype', '').lower()
        
        # Populate administrative columns from the connected hit
        g_city = addr.get('city') or addr.get('town') or addr.get('village')
        g_county = addr.get('county')
        g_state = addr.get('state')
        g_country = addr.get('country')
        
        # Primary Category
        if a_type in ['city', 'town', 'village', 'municipality']:
            found_data['City'].append(cleaned_query.split(',')[0])
        elif a_type in ['state', 'province']:
            found_data['State'].append(g_state)
        elif a_type == 'country':
            found_data['Country'].append(g_country)
        else:
            found_data['Landmark'].append(cleaned_query.split(',')[0])

        # Hierarchy Cascading
        if g_city: found_data['City'].append(g_city)
        if g_county: found_data['County'].append(g_county)
        if g_state: found_data['State'].append(g_state)
        if g_country: found_data['Country'].append(g_country)

        for key in found_data:
            row[key] = ", ".join(list(dict.fromkeys(found_data[key]))) if found_data[key] else "-"
            
        row['Areas_for_coordinates'] = cleaned_query
        row['Final_Coordinates'] = f"{loc.latitude}, {loc.longitude}"
        row['Areas'] = cleaned_query 
    else:
        row['Validation'] = "No"

    return row

# --- 5. MAIN EXECUTION ---

def main():
    print(f"Reading {INPUT_FILE} and detecting #FFFF00 highlights...")
    wb = openpyxl.load_workbook(INPUT_FILE)
    ws = wb.active
    
    # Map yellow rows by index
    yellow_map = {i: is_cell_yellow(excel_row[0]) for i, excel_row in enumerate(ws.iter_rows(min_row=2), start=0)}
    
    df = pd.read_excel(INPUT_FILE)
    print("Applying spaCy Fuzzy Matching and Geographic Connectivity logic...")
    
    # Process every row against the criteria
    df = df.apply(lambda r: process_row_intelligence(r, yellow_map.get(r.name, False)), axis=1)

    df.to_excel(OUTPUT_FILE, index=False)
    print(f"Success! Corrected coordinates and administrative data saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()