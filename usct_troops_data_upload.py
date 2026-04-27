import pandas as pd
import psycopg2
import os
import time
from dotenv import load_dotenv
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError

# Load environment variables
load_dotenv()

# --- CONFIGURATION ---
DB_CONNECTION_STRING = os.getenv("DB_CONNECTION_STRING")
# Unique User Agent to avoid being flagged by Nominatim
USER_AGENT = "genealogy_traceline_usct_processor_v2"
geolocator = Nominatim(user_agent=USER_AGENT, timeout=10)

def get_db_connection():
    if not DB_CONNECTION_STRING:
        print("Error: DB_CONNECTION_STRING not found.")
        return None
    try:
        return psycopg2.connect(DB_CONNECTION_STRING)
    except Exception as e:
        print(f"Database connection failed: {e}")
        return None

def clean_val(val):
    """Standardizes Excel inputs to None for SQL NULL."""
    if pd.isna(val): return None
    s_val = str(val).strip()
    return None if s_val.lower() in ["", "nan", "-"] else s_val

def format_date(val):
    """Formats dates for PostgreSQL, handling year-only entries."""
    cleaned = clean_val(val)
    if not cleaned: return None
    try:
        if len(cleaned) == 4 and cleaned.isdigit():
            return f"{cleaned}-01-01"
        return pd.to_datetime(cleaned).strftime('%Y-%m-%d')
    except:
        return None

def safe_geocode(query, attempts=5):
    """Handles geocoding with exponential backoff to prevent 429 errors."""
    if not query: return None
    for i in range(attempts):
        try:
            # Respectful delay: 1.5s between requests
            time.sleep(1.5) 
            return geolocator.geocode(query, addressdetails=True, language="en")
        except GeocoderServiceError as e:
            if "429" in str(e):
                wait_time = (i + 1) * 10
                print(f"Rate limited (429). Cooling down for {wait_time}s...")
                time.sleep(wait_time)
            else: return None
        except GeocoderTimedOut:
            time.sleep(2)
    return None

def setup_tables(cur):
    """Ensures the supplemental usct_Connecticut table exists with location links."""
    cur.execute("""
        CREATE TABLE IF NOT EXISTS usct_Connecticut (
            id SERIAL PRIMARY KEY,
            member_id INT REFERENCES family_members(member_id),
            pob_location_id INT REFERENCES locations(location_id),
            residence_location_id INT REFERENCES locations(location_id),
            enlistment_location_id INT REFERENCES locations(location_id),
            excel_id TEXT,
            regiment TEXT,
            company TEXT,
            enlistment_date DATE,
            age TEXT,
            occupation TEXT,
            marital_status TEXT,
            description TEXT,
            wounded TEXT,
            died_in_service TEXT,
            muster_out_date DATE,
            sign_name TEXT,
            substitute TEXT,
            substitute_for TEXT,
            source TEXT,
            box TEXT,
            folder TEXT,
            notes TEXT
        );
    """)

def get_or_insert_location(cur, city, county, state, country, coords, cache):
    """Retrieves or creates a location using a local cache to minimize DB/API hits."""
    city, county, state, country, coords = map(clean_val, [city, county, state, country, coords])
    if not any([city, state, country]): return None

    # Create a unique key for the cache
    loc_key = (city, county, state, country)
    if loc_key in cache: return cache[loc_key]

    # 1. Check DB first
    cur.execute("""
        SELECT location_id FROM locations 
        WHERE (city IS NOT DISTINCT FROM %s) 
        AND (county IS NOT DISTINCT FROM %s) 
        AND (state IS NOT DISTINCT FROM %s)
        AND (country IS NOT DISTINCT FROM %s)
    """, (city, county, state, country))
    
    res = cur.fetchone()
    if res:
        cache[loc_key] = res[0]
        return res[0]

    # 2. Geocode if coordinates are missing
    if not coords:
        query = ", ".join([p for p in [city, state, country] if p])
        loc = safe_geocode(query)
        coords = f"{loc.latitude}, {loc.longitude}" if loc else "0.0, 0.0"

    # 3. Insert and Cache
    cur.execute("""
        INSERT INTO locations (city, county, state, country, coordinates) 
        VALUES (%s, %s, %s, %s, %s) RETURNING location_id
    """, (city, county, state, country, coords))
    
    loc_id = cur.fetchone()[0]
    cache[loc_key] = loc_id
    return loc_id

def run_usct_ingestion(file_path):
    if not os.path.exists(file_path):
        print(f"File {file_path} not found.")
        return

    df = pd.read_excel(file_path)
    conn = get_db_connection()
    if not conn: return
    cur = conn.cursor()

    setup_tables(cur)
    loc_cache = {} # Persistent cache for the duration of the run

    print(f"Processing {len(df)} records. Unique locations will be geocoded as encountered.")

    for i, row in df.iterrows():
        # A. Process Three Distinct Locations
        pob_id = get_or_insert_location(cur, row.get('POB_City'), row.get('POB_County'), 
                                        row.get('POB_State'), row.get('POB_Country'), 
                                        row.get('Birth_coordinates'), loc_cache)
        
        res_id = get_or_insert_location(cur, row.get('Residence_City'), row.get('Residence_County'), 
                                        row.get('Residence_State'), row.get('Residence_Country'), 
                                        row.get('Residence_coordinates'), loc_cache)
        
        enl_id = get_or_insert_location(cur, row.get('Enlistment_City'), row.get('Enlistment_County'), 
                                        row.get('Enlistment_State'), row.get('Enlistment_Country'), 
                                        row.get('Enlistment_Coordinates'), loc_cache)

        # B. Insert into family_members
        cur.execute("""
            INSERT INTO family_members (
                first_name, last_name, alias, gender, race, ethnicity, 
                birth_date, war, branch, military_service, birth_location_id
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'Yes', %s) 
            RETURNING member_id
        """, (
            clean_val(row.get('First_name')), clean_val(row.get('Surname')), 
            clean_val(row.get('MI_name')), clean_val(row.get('Gender')), 
            clean_val(row.get('Race')), clean_val(row.get('Ethnicity')), 
            format_date(row.get('Birthdate')), clean_val(row.get('War')), 
            clean_val(row.get('Branch')), pob_id
        ))
        m_id = cur.fetchone()[0]

        # C. Insert into usct_Connecticut with all location IDs
        cur.execute("""
            INSERT INTO usct_Connecticut (
                member_id, pob_location_id, residence_location_id, enlistment_location_id,
                excel_id, regiment, company, enlistment_date, 
                age, occupation, marital_status, description, wounded, 
                died_in_service, muster_out_date, sign_name, substitute, 
                substitute_for, source, box, folder, notes
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            m_id, pob_id, res_id, enl_id,
            clean_val(row.get('ID')), clean_val(row.get('Regiment')), 
            clean_val(row.get('Company')), format_date(row.get('Enlistment_date')), 
            clean_val(row.get('Age')), clean_val(row.get('Occupation')), 
            clean_val(row.get('Marital_status')), clean_val(row.get('Description')), 
            clean_val(row.get('Wounded')), clean_val(row.get('Died_in_service')), 
            format_date(row.get('Muster_out_date')), clean_val(row.get('Sign_name')), 
            clean_val(row.get('Substitue')), clean_val(row.get('Substitute_for')), 
            clean_val(row.get('Source')), clean_val(row.get('Box')), 
            clean_val(row.get('Folder')), clean_val(row.get('Notes'))
        ))

        # Commit in batches for performance
        if i % 20 == 0:
            print(f"Progress: {i}/{len(df)} rows processed...")
            conn.commit()

    conn.commit()
    cur.close()
    conn.close()
    print("Ingestion complete. Data is now searchable by Birth, Residence, and Enlistment locations.")

if __name__ == "__main__":
    # Update with your local Excel file path
    run_usct_ingestion('USCT_Connecticut_Data.xlsx')