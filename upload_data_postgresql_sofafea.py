import pandas as pd
import psycopg2
import os

# --- AZURE CONNECTION CONFIGURATION ---
# Ensure your credentials are correct. URL-encode special characters in the password.
DB_CONNECTION_STRING = "postgresql://genealogy_user:Bl%40ckLiveSMaTTeR324.@sofafea-postgres.postgres.database.azure.com/postgres?sslmode=require"

def get_db_connection():
    try:
        return psycopg2.connect(DB_CONNECTION_STRING)
    except Exception as e:
        print(f"Failed to connect to Azure: {e}")
        return None

def format_date(val):
    """Converts years or strings to valid Postgres DATE format."""
    if pd.isna(val) or str(val).strip() == "" or str(val).lower() == 'nan':
        return None
    try:
        # Check if it's a year-only integer (e.g., 1776)
        if isinstance(val, (int, float)) or str(val).isdigit():
            return f"{int(float(val))}-01-01"
    except: pass
    return str(val).strip()

def clean_val(val):
    """Converts NaN to None and strips strings."""
    if pd.isna(val) or str(val).strip() == "" or str(val).lower() == 'nan':
        return None
    return str(val).strip()

def get_or_insert_location(cur, city, county, state, coords, country="United States", landmark="-"):
    """Normalized location handler with hardcoded defaults."""
    city, county, state, coords = map(clean_val, [city, county, state, coords])
    if not city and not coords: return None

    cur.execute("""
        SELECT location_id FROM locations 
        WHERE (city IS NOT DISTINCT FROM %s) AND (county IS NOT DISTINCT FROM %s) 
        AND (state IS NOT DISTINCT FROM %s) AND (country IS NOT DISTINCT FROM %s)
        AND (landmark IS NOT DISTINCT FROM %s) AND (coordinates IS NOT DISTINCT FROM %s)
    """, (city, county, state, country, landmark, coords))
    
    res = cur.fetchone()
    if res: return res[0]

    cur.execute("""
        INSERT INTO locations (city, county, state, country, landmark, coordinates) 
        VALUES (%s, %s, %s, %s, %s, %s) RETURNING location_id
    """, (city, county, state, country, landmark, coords))
    return cur.fetchone()[0]

def run_genealogy_ingestion(file_path):
    if not os.path.exists(file_path):
        print(f"File {file_path} not found.")
        return

    df = pd.read_excel(file_path)
    conn = get_db_connection()
    if not conn: return
    cur = conn.cursor()

    # --- BLOCK TRACKING LOGIC ---
    current_gen = 1
    # These are the parents for the rows currently being processed
    active_parents = {"father": None, "mother": None}
    # This stores the first couple of the current block to become parents for the NEXT block
    potential_parents_for_next_gen = {"father": None, "mother": None}

    print(f"Starting ingestion. Top rows assigned to Generation {current_gen}...")

    for i, row in df.iterrows():
        # Check if the row is empty (Generation Separator)
        is_empty = row.isnull().all()
        # Also check if the first cell is a number (alternative separator)
        is_num_marker = str(row.iloc[0]).strip().isdigit()

        if is_empty or is_num_marker:
            current_gen += 1
            # Move the previous block's "first couple" into the active parent slot
            active_parents = potential_parents_for_next_gen.copy()
            # Reset potential parents for the new block we are about to enter
            potential_parents_for_next_gen = {"father": None, "mother": None}
            print(f"Separator detected. Moving to Generation {current_gen}...")
            continue

        # --- 1. PROCESS SOLDIER ---
        s_loc_id = get_or_insert_location(cur, row.get('City'), row.get('County'), 
                                          row.get('State'), row.get('Coordinates'))

        cur.execute("""
            INSERT INTO family_members (
                first_name, last_name, alias, generation_number, 
                father_id, mother_id, birth_date, birth_location_id, 
                race, ethnicity, military_service, branch, war,
                death_date, marriage_date, gender
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'Male') 
            RETURNING member_id
        """, (
            clean_val(row.get('First_Name')), clean_val(row.get('Last_Name')), clean_val(row.get('Alias')), 
            current_gen, active_parents['father'], active_parents['mother'], 
            format_date(row.get('Gen_1_Birth_Date')), s_loc_id, 
            clean_val(row.get('Race')), clean_val(row.get('Ethnicity')), 
            clean_val(row.get('Military_Service')), clean_val(row.get('Branch')), clean_val(row.get('War')), 
            format_date(row.get('Gen_1_Death_Date')), format_date(row.get('Gen_1_Marriage_Date'))
        ))
        soldier_id = cur.fetchone()[0]

        # --- 2. PROCESS SPOUSE ---
        sp_loc_id = get_or_insert_location(cur, row.get('City.1'), row.get('County.1'), 
                                           row.get('State.1'), row.get('Coordinates.1'))

        spouse_first = clean_val(row.get('Gen_1_Spouse_First_Name'))
        spouse_id = None
        
        if spouse_first:
            cur.execute("""
                INSERT INTO family_members (
                    first_name, last_name, alias, generation_number, 
                    birth_date, birth_location_id, race, ethnicity, death_date, gender
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'Female') 
                RETURNING member_id
            """, (
                spouse_first, clean_val(row.get('Gen_1_Spouse_Surname/Maiden_Name')), 
                clean_val(row.get('Alias.1')), current_gen, 
                format_date(row.get('Spouse_Gen_1_Birth_Date')), 
                sp_loc_id, clean_val(row.get('Race.1')), clean_val(row.get('Ethnicity.1')), 
                format_date(row.get('Spouse_Gen_1_Death_Date'))
            ))
            spouse_id = cur.fetchone()[0]

        # --- 3. STORE POTENTIAL PARENTS ---
        # We capture the first couple of this generation to be the parents for the children in the next block
        if potential_parents_for_next_gen['father'] is None:
            potential_parents_for_next_gen = {"father": soldier_id, "mother": spouse_id}

    conn.commit()
    cur.close()
    conn.close()
    print("Ingestion complete. Family tree successfully reconstructed.")

if __name__ == "__main__":
    run_genealogy_ingestion('Ancestors Database_v2_copy.xlsx')