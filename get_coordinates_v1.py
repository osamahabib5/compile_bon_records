import pandas as pd
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
import os

# --- SETTINGS ---
INPUT_FILE = 'Ancestors Database_v2_copy.xlsx'
OUTPUT_FILE = 'Ancestors Database_v3.xlsx'
# The specific string to look for in column headers
TARGET_COL_NAME = 'City, County, State' 

def process_all_locations():
    if not os.path.exists(INPUT_FILE):
        print(f"Error: {INPUT_FILE} not found.")
        return

    # Load the Excel file - engine='openpyxl' handles .xlsx best
    df = pd.read_excel(INPUT_FILE)

    # 1. Identify all columns that match our target name
    # Pandas handles duplicates by appending .1, .2, etc. 
    # We find all columns that START with our target string.
    location_cols = [col for col in df.columns if col.startswith(TARGET_COL_NAME)]
    
    print(f"Found {len(location_cols)} location-based columns to process.")

    # 2. Geocoding Setup with Caching
    geolocator = Nominatim(user_agent="genealogy_mapper_v3")
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1.2)
    location_cache = {}

    def get_coords(loc_string):
        if pd.isna(loc_string) or str(loc_string).strip() == "":
            return None
        loc_string = str(loc_string).strip()
        if loc_string in location_cache:
            return location_cache[loc_string]
        try:
            location = geocode(loc_string)
            if location:
                coords = f"{location.latitude}, {location.longitude}"
                location_cache[loc_string] = coords
                return coords
        except:
            return None
        return None

    # 3. Scrape every row and process every relevant column
    for col in location_cols:
        print(f"Processing column: {col}...")
        
        # Create new column names based on the original (e.g., 'City_1', 'City_2')
        suffix = col.replace(TARGET_COL_NAME, "")
        city_label = f"City{suffix}"
        county_label = f"County{suffix}"
        state_label = f"State{suffix}"
        coord_label = f"Coordinates{suffix}"

        # Split the data
        split_data = df[col].astype(str).str.split(',', expand=True)
        
        # Assign split values (handling cases where a row might be missing a comma)
        df[city_label] = split_data[0].str.strip() if 0 in split_data else ""
        df[county_label] = split_data[1].str.strip() if 1 in split_data else ""
        df[state_label] = split_data[2].str.strip() if 2 in split_data else ""

        # Populate coordinates for this specific instance
        df[coord_label] = df[col].apply(get_coords)

    # 4. Save the full results
    try:
        # We save everything, including original columns and the new categorized ones
        df.to_excel(OUTPUT_FILE, index=False)
        print(f"Success! All rows scraped. Result saved to: {OUTPUT_FILE}")
    except Exception as e:
        print(f"Error saving file: {e}")

if __name__ == "__main__":
    process_all_locations()