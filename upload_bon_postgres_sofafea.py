import pandas as pd
import psycopg2
import os
import time
from dotenv import load_dotenv
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

# Load variables from .env file
load_dotenv()

# --- CONNECTION CONFIGURATION ---
DB_CONNECTION_STRING = os.getenv("DB_CONNECTION_STRING")

# Initialize Geocoder
geolocator = Nominatim(user_agent="genealogy_data_ingestor")
geocode_service = RateLimiter(geolocator.geocode, min_delay_seconds=1)

def get_db_connection():
    if not DB_CONNECTION_STRING:
        print("Error: DB_CONNECTION_STRING not found in environment.")
        return None
    try:
        return psycopg2.connect(DB_CONNECTION_STRING)
    except Exception as e:
        print(f"Failed to connect to Azure: {e}")
        return None

def clean_val(val):
    """Standardizes Excel placeholders to None."""
    if pd.isna(val):
        return None
    s_val = str(val).strip()
    if s_val == "" or s_val.lower() == 'nan' or s_val == '-':
        return None
    return s_val

def format_date(val):
    """Formats dates for PostgreSQL ingestion."""
    cleaned = clean_val(val)
    if cleaned is None:
        return None
    try:
        if cleaned.isdigit() and len(cleaned) == 4:
            return f"{cleaned}-01-01"
    except: 
        pass
    return cleaned

def fetch_geopy_coords(state, country):
    """External fallback: retrieves coordinates via Geopy."""
    st = clean_val(state)
    co = clean_val(country)
    
    # Extract first state name (handles "New York" or "Delaware, Virginia")
    search_state = st.split(',')[0].strip() if st else None
    query_parts = [p for p in [search_state, co] if p]
    
    if not query_parts:
        return None
        
    query = ", ".join(query_parts)
    try:
        location = geocode_service(query)
        if location:
            return f"{location.latitude}, {location.longitude}"
    except Exception as e:
        print(f"Geopy error for {query}: {e}")
    return None

def find_coords_in_mapping(state, country, areas_str, departure_str):
    """Internal Priority: Matches State/Country combination against pipe-separated mappings."""
    st = clean_val(state)
    co = clean_val(country)
    areas = clean_val(areas_str)
    deps = clean_val(departure_str)
    
    if not st or not co or not areas or not deps:
        return None

    # Identify target string (e.g., "New York, United States")
    first_st = st.split(',')[0].strip()
    target = f"{first_st}, {co}"
    
    areas_list = [a.strip() for a in areas.split('|')]
    deps_list = [d.strip() for d in deps.split('|')]
    
    try:
        if target in areas_list:
            idx = areas_list.index(target)
            if idx < len(deps_list):
                coord_match = deps_list[idx].strip()
                if coord_match and coord_match != "-":
                    return coord_match
    except:
        pass
    return None

def get_or_insert_location(cur, city, county, state, country, landmark, coords, areas_map, deps_map):
    """Manages location insertion with hierarchical coordinate lookup."""
    city, county, state, country, landmark, coords = map(clean_val, [city, county, state, country, landmark, coords])
    
    # 1. Coordinate Retrieval Logic
    if not coords:
        # Try Internal Mapping first
        coords = find_coords_in_mapping(state, country, areas_map, deps_map)
        
        # Fallback to Geopy if Mapping failed
        if not coords:
            coords = fetch_geopy_coords(state, country)
    
    # Final default to satisfy NOT NULL constraints
    if not coords:
        coords = "0.0, 0.0"

    if not any([city, county, state]):
        return None

    # Check for existing location using unique constraint components
    cur.execute("""
        SELECT location_id FROM locations 
        WHERE (city IS NOT DISTINCT FROM %s) 
        AND (county IS NOT DISTINCT FROM %s) 
        AND (state IS NOT DISTINCT FROM %s)
    """, (city, county, state))
    
    res = cur.fetchone()
    if res:
        return res[0]

    cur.execute("""
        INSERT INTO locations (city, county, state, country, landmark, coordinates) 
        VALUES (%s, %s, %s, %s, %s, %s) RETURNING location_id
    """, (city, county, state, country, landmark, coords))
    return cur.fetchone()[0]

def get_or_insert_member(cur, first_name, last_name, gen, gender):
    """Maintains unique records for family tree members."""
    f_name, l_name = clean_val(first_name), clean_val(last_name)
    if not f_name: return None
    
    cur.execute("""
        SELECT member_id FROM family_members 
        WHERE (LOWER(first_name) = LOWER(%s)) 
        AND (LOWER(last_name) IS NOT DISTINCT FROM LOWER(%s)) 
        AND (generation_number = %s)
    """, (f_name, l_name, gen))
    
    res = cur.fetchone()
    if res: return res[0]
    
    cur.execute("""
        INSERT INTO family_members (first_name, last_name, generation_number, gender) 
        VALUES (%s, %s, %s, %s) RETURNING member_id
    """, (f_name, l_name, gen, gender))
    return cur.fetchone()[0]

def run_genealogy_ingestion(file_path):
    if not os.path.exists(file_path):
        print(f"File {file_path} not found.")
        return

    df = pd.read_excel(file_path)
    df.columns = df.columns.astype(str).str.strip()
    
    conn = get_db_connection()
    if not conn: return
    cur = conn.cursor()

    for i, row in df.iterrows():
        areas_map = row.get('Areas_for_coordinates')
        deps_map = row.get('Departure_Coordinates')

        # 1. Determine Generation
        has_grand = clean_val(row.get('GrandMother_FirstName')) or clean_val(row.get('GrandMother_Surname'))
        has_parents = any([clean_val(row.get('Father_FirstName')), clean_val(row.get('Mother_FirstName'))])
        
        if has_grand:
            sub_gen, p_gen, g_gen = 3, 2, 1
        elif has_parents:
            sub_gen, p_gen, g_gen = 2, 1, None
        else:
            sub_gen, p_gen, g_gen = 1, None, None

        # 2. Ancestor Processing
        gm_id = get_or_insert_member(cur, row.get('GrandMother_FirstName'), row.get('GrandMother_Surname'), g_gen, 'Female') if g_gen else None
        f_id = get_or_insert_member(cur, row.get('Father_FirstName'), row.get('Father_Surname'), p_gen, 'Male') if p_gen else None
        m_id = get_or_insert_member(cur, row.get('Mother_FirstName'), row.get('Mother_Surname'), p_gen, 'Female') if p_gen else None

        if m_id and gm_id:
            cur.execute("UPDATE family_members SET mother_id = %s WHERE member_id = %s", (gm_id, m_id))
        if f_id and m_id:
            cur.execute("UPDATE family_members SET spouse_id = %s WHERE member_id = %s", (m_id, f_id))
            cur.execute("UPDATE family_members SET spouse_id = %s WHERE member_id = %s", (f_id, m_id))

        # 3. Location Ingestion
        loc_id = get_or_insert_location(cur, row.get('City'), row.get('County'), row.get('State'), 
                                        row.get('Country'), row.get('Landmark'), row.get('Final_Coordinates'),
                                        areas_map, deps_map)
        
        arr_loc_id = get_or_insert_location(cur, row.get('Arrival_Port'), None, None, 
                                            row.get('Arrival_Port_Country'), None, row.get('Arrival_Coordinates'),
                                            areas_map, deps_map)

        # 4. Subject Ingestion
        cur.execute("""
            INSERT INTO family_members (
                first_name, last_name, alias, generation_number, 
                father_id, mother_id, birth_date, birth_location_id, 
                gender, race, ethnicity
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
            RETURNING member_id
        """, (
            clean_val(row.get('First_Name')), clean_val(row.get('Surname')), '-', 
            sub_gen, f_id, m_id, format_date(row.get('Birthdate')), 
            loc_id, clean_val(row.get('Gender')), clean_val(row.get('Race')), clean_val(row.get('Ethnicity'))
        ))
        subject_id = cur.fetchone()[0]

        # 5. Metadata Ingestion
        cur.execute("""
            INSERT INTO book_of_negroes (
                member_id, excel_id, book, ship_name, notes, ship_notes, 
                origin, areas_for_coordinates, departure_port, departure_date, 
                ref_page, commander, enslaver, primary_source_1, primary_source_2, arrival_location_id
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            subject_id, clean_val(row.get('ID')), clean_val(row.get('Book')), clean_val(row.get('Ship_Name')), 
            clean_val(row.get('Notes')), clean_val(row.get('Ship_Notes')), clean_val(row.get('Origin')), 
            clean_val(row.get('Areas_for_coordinates')), clean_val(row.get('Departure_Port')), 
            clean_val(row.get('Departure_Date')), clean_val(row.get('Ref_Page')), 
            clean_val(row.get('Commander')), clean_val(row.get('Enslaver')), 
            clean_val(row.get('Primary_Source_1')), clean_val(row.get('Primary_Source_2')), arr_loc_id
        ))

        if i % 10 == 0:
            print(f"Processed row {i}...")

    conn.commit()
    cur.close()
    conn.close()
    print("Ingestion complete.")

if __name__ == "__main__":
    run_genealogy_ingestion('Consolidated_Book_of_Negroes_v11.xlsx')