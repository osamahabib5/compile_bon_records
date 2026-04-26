import pandas as pd
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
import os

# --- SETTINGS ---
INPUT_FILE = 'USCTs_Connecticut_rev_03.xlsx'
OUTPUT_FILE = 'USCTs_Connecticut_rev_03_copy.xlsx'

def get_modified_lineage_data():
    if not os.path.exists(INPUT_FILE):
        print(f"Error: {INPUT_FILE} not found.")
        return

    df = pd.read_excel(INPUT_FILE)
    
    # Verify required columns exist
    required = ['Residence_City', 'Residence_State', 'Enlistment_City', 'Enlistment_State', 'Place_of_birth']
    for col in required:
        if col not in df.columns:
            print(f"Error: Column '{col}' not found in the file.")
            return

    # Initialize Geocoder
    geolocator = Nominatim(user_agent="genealogy_mapper_v5", timeout=10)
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1.5)

    # Global cache for (City, State) pairs to be used across both Residence and Enlistment
    location_cache = {} # (city, state) -> (county, country, coords)
    birth_cache = {}    # place_string -> coords

    # Pre-populate cache with any existing data in the file to avoid re-querying
    if 'Residence_coordinates' in df.columns:
        for idx, row in df.iterrows():
            key = (str(row['Residence_City']).strip(), str(row['Residence_State']).strip())
            if pd.notna(row['Residence_coordinates']) and key not in location_cache:
                location_cache[key] = (
                    row.get('Residence_County'), 
                    row.get('Residence_Country'), 
                    row['Residence_coordinates']
                )

    def fetch_geodata(city, state):
        city = str(city).strip() if pd.notna(city) else ""
        state = str(state).strip() if pd.notna(state) else ""
        
        if not city and not state:
            return None, None, None
            
        key = (city, state)
        if key in location_cache:
            return location_cache[key]

        try:
            # Try structured USA query first
            location = geocode(query={'city': city, 'state': state, 'country': 'USA'}, addressdetails=True)
            if not location:
                location = geocode(query={'city': city, 'state': state}, addressdetails=True)
            
            if location:
                addr = location.raw.get('address', {})
                # Extract specific administrative levels
                county = addr.get('county', '')
                country = addr.get('country', '')
                coords = f"{location.latitude}, {location.longitude}"
                
                location_cache[key] = (county, country, coords)
                return county, country, coords
        except Exception as e:
            print(f"Error geocoding ({city}, {state}): {e}")
        
        location_cache[key] = (None, None, None)
        return None, None, None

    # Lists to store the new column data
    res_counties, res_countries, res_coords = [], [], []
    en_counties, en_countries, en_coords = [], [], []
    birth_coords = []

    print("Processing records... This may take time due to API rate limits.")

    for idx, row in df.iterrows():
        # 1. Process Residence
        # Skip API if Residence_coordinates is already populated
        if 'Residence_coordinates' in df.columns and pd.notna(row['Residence_coordinates']):
            r_county, r_country, r_coord = row.get('Residence_County'), row.get('Residence_Country'), row['Residence_coordinates']
        else:
            r_county, r_country, r_coord = fetch_geodata(row['Residence_City'], row['Residence_State'])
        
        res_counties.append(r_county)
        res_countries.append(r_country)
        res_coords.append(r_coord)

        # 2. Process Enlistment
        e_city = str(row['Enlistment_City']).strip()
        e_state = str(row['Enlistment_State']).strip()
        r_city = str(row['Residence_City']).strip()
        r_state = str(row['Residence_State']).strip()

        # Check if Enlistment matches Residence for this row
        if e_city == r_city and e_state == r_state:
            en_counties.append(r_county)
            en_countries.append(r_country)
            en_coords.append(r_coord)
        else:
            e_county, e_country, e_coord = fetch_geodata(e_city, e_state)
            en_counties.append(e_county)
            en_countries.append(e_country)
            en_coords.append(e_coord)

        # 3. Process Birth
        p_birth = str(row['Place_of_birth']).strip() if pd.notna(row['Place_of_birth']) else ""
        if not p_birth:
            birth_coords.append(None)
        elif p_birth in birth_cache:
            birth_coords.append(birth_cache[p_birth])
        else:
            try:
                loc = geocode(p_birth)
                b_coord = f"{loc.latitude}, {loc.longitude}" if loc else None
                birth_cache[p_birth] = b_coord
                birth_coords.append(b_coord)
            except:
                birth_coords.append(None)

    # --- Column Reorganization ---
    # Helper to safely insert columns
    def safe_insert(df, after_col, new_col_name, data):
        if new_col_name in df.columns:
            df[new_col_name] = data # Update if exists
        else:
            idx = df.columns.get_loc(after_col)
            df.insert(idx + 1, new_col_name, data)

    # Residence Inserts
    safe_insert(df, 'Residence_City', 'Residence_County', res_counties)
    safe_insert(df, 'Residence_State', 'Residence_Country', res_countries)
    safe_insert(df, 'Residence_Country', 'Residence_coordinates', res_coords)

    # Enlistment Inserts
    safe_insert(df, 'Enlistment_City', 'Enlistment_County', en_counties)
    safe_insert(df, 'Enlistment_State', 'Enlistment_Country', en_countries)
    safe_insert(df, 'Enlistment_Country', 'Enlistment_Coordinates', en_coords)

    # Birth Insert
    safe_insert(df, 'Place_of_birth', 'Birth_coordinates', birth_coords)

    df.to_excel(OUTPUT_FILE, index=False)
    print(f"Success! Result saved to: {OUTPUT_FILE}")

if __name__ == "__main__":
    get_modified_lineage_data()