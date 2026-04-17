import pandas as pd
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
import time

# --- 1. SETUP ---
INPUT_FILE = 'Consolidated_Directory_v13_draft.xlsx'
OUTPUT_FILE = 'Geocoded_Maritime_Split.xlsx'

geolocator = Nominatim(user_agent="maritime_split_geocoder_v24")
geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1.1, error_wait_seconds=5)

df = pd.read_excel(INPUT_FILE)

# --- 2. LOGIC FOR QUERY GENERATION ---
def generate_queries(row):
    port = str(row.get('Arrival_Port', '')).strip()
    # Handle NaN or empty strings
    if port.lower() in ['nan', '-', '']: port = None
    
    countries_raw = str(row.get('Arrival_Port_Country', '')).strip()
    if countries_raw.lower() in ['nan', '-', '']: return []
    
    # Split by comma to handle "United Kingdom, Germany"
    country_list = [c.strip() for c in countries_raw.split(',') if c.strip()]
    
    queries = []
    for i, country in enumerate(country_list):
        # RULE: Only pair the port with the FIRST country in the list
        if i == 0 and port:
            queries.append(f"{port}, {country}")
        else:
            # Subsequent entries or cases with no port use only the country
            queries.append(country)
    return queries

# Apply the logic to create a helper list of queries per row
df['Query_List'] = df.apply(generate_queries, axis=1)

# --- 3. UNIQUE GEOCODING ENGINE ---
# Flatten the list of lists to get all unique strings needed for the API
all_unique_queries = pd.Series([q for sublist in df['Query_List'] for q in sublist]).unique()

print(f"Unique locations to geocode: {len(all_unique_queries)}")

geo_map = {}
for q in all_unique_queries:
    try:
        location = geocode(q)
        geo_map[q] = f"{location.latitude}, {location.longitude}" if location else "-"
    except:
        geo_map[q] = "-"

# --- 4. MAPPING BACK WITH PARTITIONING ---
def map_to_coordinates(query_list):
    if not query_list:
        return "-"
    # Map each query in the list to its cached coordinate
    coords = [geo_map.get(q, "-") for q in query_list]
    # Join multiple coordinates with the double dash partition
    return " -- ".join(coords)

df['Arrival_Coordinates'] = df['Query_List'].apply(map_to_coordinates)

# --- 5. CLEANUP & EXPORT ---
df = df.drop(columns=['Query_List'])
df.to_excel(OUTPUT_FILE, index=False)
print(f"Success! Partitioned coordinates saved to {OUTPUT_FILE}")