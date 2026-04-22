import pandas as pd
import psycopg2
import os

# --- AZURE CONNECTION CONFIGURATION ---
DB_CONNECTION_STRING = "postgresql://genealogy_user:Bl%40ckLiveSMaTTeR324.@sofafea-postgres.postgres.database.azure.com/postgres?sslmode=require"

def get_db_connection():
    try:
        return psycopg2.connect(DB_CONNECTION_STRING)
    except Exception as e:
        print(f"Failed to connect to Azure: {e}")
        return None

def format_date(val):
    if pd.isna(val) or str(val).strip() == "" or str(val).lower() == 'nan':
        return None
    try:
        if isinstance(val, (int, float)) or str(val).isdigit():
            return f"{int(float(val))}-01-01"
    except: pass
    return str(val).strip()

def clean_val(val):
    if pd.isna(val) or str(val).strip() == "" or str(val).lower() == 'nan':
        return None
    return str(val).strip()

def get_or_insert_location(cur, city, county, state, coords, country="United States", landmark="-"):
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
    df.columns = df.columns.astype(str).str.strip()
    
    conn = get_db_connection()
    if not conn: return
    cur = conn.cursor()

    current_gen = 1
    active_parents = {"father": None, "mother": None}
    potential_parents_for_next_gen = {"father": None, "mother": None}
    
    records_inserted = 0

    for i, row in df.iterrows():
        first_name = clean_val(row.get('First_Name'))
        last_name = clean_val(row.get('Last_Name'))
        
        # Generation Break detection
        if not first_name and not last_name:
            if potential_parents_for_next_gen['father'] is not None:
                current_gen += 1
                active_parents = potential_parents_for_next_gen.copy()
                potential_parents_for_next_gen = {"father": None, "mother": None}
            continue

        # --- 1. INSERT SOLDIER ---
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
            first_name, last_name, clean_val(row.get('Alias')), 
            current_gen, active_parents['father'], active_parents['mother'], 
            format_date(row.get('Gen_1_Birth_Date')), s_loc_id, 
            clean_val(row.get('Race')), clean_val(row.get('Ethnicity')), 
            clean_val(row.get('Military_Service')), clean_val(row.get('Branch')), clean_val(row.get('War')), 
            format_date(row.get('Gen_1_Death_Date')), format_date(row.get('Gen_1_Marriage_Date'))
        ))
        soldier_id = cur.fetchone()[0]

        # --- 2. INSERT SPOUSE ---
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

            # --- 3. LINK SPOUSES ---
            # Now that both IDs exist, we update both records to point to each other
            cur.execute("UPDATE family_members SET spouse_id = %s WHERE member_id = %s", (spouse_id, soldier_id))
            cur.execute("UPDATE family_members SET spouse_id = %s WHERE member_id = %s", (soldier_id, spouse_id))

        # Store potential parents for next block
        if potential_parents_for_next_gen['father'] is None:
            potential_parents_for_next_gen = {"father": soldier_id, "mother": spouse_id}
        
        records_inserted += 1
        print(f"Row {i+2}: Processed {first_name} {last_name} and spouse.")

    conn.commit()
    cur.close()
    conn.close()
    print(f"Ingestion complete. {records_inserted} records linked successfully.")

if __name__ == "__main__":
    run_genealogy_ingestion('Ancestors Database_v2_copy.xlsx')