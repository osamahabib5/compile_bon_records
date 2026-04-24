import pandas as pd
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
import os

# --- SETTINGS ---
INPUT_FILE = 'USCTs_Connecticut_rev_02_copy.xlsx'
OUTPUT_FILE = 'USCTs_Connecticut_rev_03.xlsx'

def get_modified_lineage_data():
    if not os.path.exists(INPUT_FILE):
        print(f"Error: {INPUT_FILE} not found.")
        return

    # Load the Excel file
    df = pd.read_excel(INPUT_FILE)
    
    # Verify required columns exist
    required = ['Residence_City', 'Residence_State', 'Place_of_birth']
    for col in required:
        if col not in df.columns:
            print(f"Error: Column '{col}' not found in the file.")
            return

    # --- Geocoding Setup ---
    geolocator = Nominatim(user_agent="genealogy_mapper_v4")
    # Rate limiter set to 1.5s to be safe with Nominatim's usage policy
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1.5)

    # Caches to prevent duplicate API calls for identical locations
    res_cache = {}   # (city, state) -> (county, country, coords)
    birth_cache = {} # place_string -> coords

    def process_residence(city, state):
        city = str(city).strip() if pd.notna(city) else ""
        state = str(state).strip() if pd.notna(state) else ""
        
        if not city and not state:
            return None, None, None
            
        key = (city, state)
        if key in res_cache:
            return res_cache[key]

        # 1. Try USA first using structured query
        try:
            location = geocode(query={'city': city, 'state': state, 'country': 'USA'}, addressdetails=True)
            
            # 2. If not found in USA, try global search
            if not location:
                location = geocode(query={'city': city, 'state': state}, addressdetails=True)
            
            if location:
                addr = location.raw.get('address', {})
                county = addr.get('county', '')
                country = addr.get('country', '')
                coords = f"{location.latitude}, {location.longitude}"
                
                res_cache[key] = (county, country, coords)
                return county, country, coords
        except Exception as e:
            print(f"Error geocoding Residence ({city}, {state}): {e}")
        
        res_cache[key] = (None, None, None)
        return None, None, None

    def process_birth(place):
        place = str(place).strip() if pd.notna(place) else ""
        if not place:
            return None
            
        if place in birth_cache:
            return birth_cache[place]
            
        try:
            location = geocode(place)
            if location:
                coords = f"{location.latitude}, {location.longitude}"
                birth_cache[place] = coords
                return coords
        except Exception as e:
            print(f"Error geocoding Birth Place ({place}): {e}")
            
        birth_cache[place] = None
        return None

    # --- Processing Data ---
    print("Geocoding Residence and Birth data (this may take time due to rate limits)...")
    
    # We use a temporary list to avoid changing df size during iteration
    res_results = [process_residence(c, s) for c, s in zip(df['Residence_City'], df['Residence_State'])]
    birth_results = [process_birth(p) for p in df['Place_of_birth']]

    # Extract results into individual lists
    counties, countries, res_coords = zip(*res_results)

    # --- Column Reorganization ---
    # 1. Insert Residence columns
    # Find position of Residence_City to place County after it
    city_idx = df.columns.get_loc('Residence_City')
    df.insert(city_idx + 1, 'Residence_County', counties)
    
    # Find position of Residence_State (now shifted by 1) to place Country/Coords next to it
    state_idx = df.columns.get_loc('Residence_State')
    df.insert(state_idx + 1, 'Residence_Country', countries)
    df.insert(state_idx + 2, 'Residence_coordinates', res_coords)

    # 2. Insert Birth column
    birth_idx = df.columns.get_loc('Place_of_birth')
    df.insert(birth_idx + 1, 'Birth_coordinates', birth_results)

    # --- Save Result ---
    try:
        df.to_excel(OUTPUT_FILE, index=False)
        print(f"Success! Result saved to: {OUTPUT_FILE}")
    except Exception as e:
        print(f"Error saving file: {e}")

if __name__ == "__main__":
    get_modified_lineage_data()