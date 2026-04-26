import pandas as pd
import psycopg2
from psycopg2.extras import DictCursor
import os

# --- CONFIGURATION ---
# Use your connection string here
DB_URL = os.environ.get("DATABASE_URL", "postgresql://genealogy_user:Bl%40ckLiveSMaTTeR324.@sofafea-postgres.postgres.database.azure.com/postgres?sslmode=require")
EXCEL_FILE_PATH = 'Ancestors Database_v2_copy.xlsx'

def clean_val(val, default=None):
    """Standardizes inputs, converting placeholders to None or a default."""
    if pd.isna(val) or val is None:
        return default
    v = str(val).strip()
    if v in ["", "-", "None", "NaN", "nan"]:
        return default
    return v

def format_date(val):
    """Formats dates or year-only integers into YYYY-MM-DD."""
    cleaned = clean_val(val)
    if not cleaned:
        return None
    try:
        if cleaned.isdigit() or (isinstance(val, (int, float))):
            return f"{int(float(val))}-01-01"
    except:
        pass
    return cleaned

def get_or_create_location(cursor, city, county, state, country, landmark, coordinates):
    """
    Retrieves or creates a location. 
    Matches Excel logic: defaults country to 'United States' and landmark to '-' if missing.
    """
    c_city = clean_val(city)
    c_county = clean_val(county)
    c_state = clean_val(state)
    c_country = clean_val(country, default="United States")
    c_land = clean_val(landmark, default="-")
    c_coords = clean_val(coordinates)

    # Skip if essential fields are missing
    if not any([c_city, c_coords]):
        return None

    # Check for existing record using IS NOT DISTINCT FROM to handle NULLs
    check_query = """
        SELECT location_id FROM public.locations 
        WHERE city IS NOT DISTINCT FROM %s 
          AND county IS NOT DISTINCT FROM %s 
          AND state IS NOT DISTINCT FROM %s
          AND country IS NOT DISTINCT FROM %s
          AND landmark IS NOT DISTINCT FROM %s
          AND coordinates IS NOT DISTINCT FROM %s
    """
    cursor.execute(check_query, (c_city, c_county, c_state, c_country, c_land, c_coords))
    result = cursor.fetchone()
    
    if result:
        return result['location_id']

    # Insert if not found
    try:
        insert_query = """
            INSERT INTO public.locations (city, county, state, country, landmark, coordinates)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING location_id;
        """
        cursor.execute(insert_query, (c_city, c_county, c_state, c_country, c_land, c_coords))
        return cursor.fetchone()['location_id']
    except psycopg2.errors.UniqueViolation:
        cursor.connection.rollback()
        cursor.execute(check_query, (c_city, c_county, c_state, c_country, c_land, c_coords))
        res = cursor.fetchone()
        return res['location_id'] if res else None

def get_or_create_member(cursor, first_name, last_name, generation, gender=None, race=None, 
                         ethnicity=None, birth_date=None, birth_loc_id=None, father_id=None, 
                         mother_id=None, directory_id=None, alias=None):
    """
    Creates a member and returns ID. Maps 'last_name' input to the 'surname' column per request.
    """
    f_name = clean_val(first_name)
    l_name = clean_val(last_name)
    
    if not f_name and not l_name: 
        return None

    # Check for existing member in same generation to prevent duplicates
    cursor.execute("""
        SELECT member_id FROM public.family_members 
        WHERE (LOWER(first_name) IS NOT DISTINCT FROM LOWER(%s)) 
        AND (LOWER(last_name) IS NOT DISTINCT FROM LOWER(%s)) 
        AND (generation_number = %s)
    """, (f_name, l_name, generation))
    
    res = cursor.fetchone()
    if res: 
        return res['member_id']
    
    # Insert new family member (Note: using 'surname' column as requested)
    insert_query = """
        INSERT INTO public.family_members (
            first_name, last_name, alias, generation_number, gender, race, ethnicity,
            birth_date, birth_location_id, father_id, mother_id, directory_id
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        ) RETURNING member_id;
    """
    cursor.execute(insert_query, (
        f_name, l_name, alias, generation, gender, race, ethnicity, 
        birth_date, birth_loc_id, father_id, mother_id, directory_id
    ))
    return cursor.fetchone()['member_id']

# --- PHASE 1: EXCEL INGESTION ---

def run_excel_ingestion(cursor):
    print(f"--- Phase 1: Ingesting Excel ({EXCEL_FILE_PATH}) ---")
    if not os.path.exists(EXCEL_FILE_PATH):
        print("Excel file not found. Skipping Phase 1.")
        return

    df = pd.read_excel(EXCEL_FILE_PATH)
    current_gen = 1
    in_gap = False

    for i, row in df.iterrows():
        f_name = clean_val(row.get('First_Name'))
        l_name = clean_val(row.get('Last_Name'))

        if not f_name and not l_name:
            if not in_gap:
                current_gen += 1
                in_gap = True
            continue
        in_gap = False

        # Parents (Gen - 1)
        fid = get_or_create_member(cursor, row.get('Father_FirstName'), row.get('Father_Surname'), current_gen - 1, gender='Male')
        mid = get_or_create_member(cursor, row.get('Mother_FirstName'), row.get('Mother_Surname'), current_gen - 1, gender='Female')

        # Location
        loc_id = get_or_create_location(
            cursor, row.get('City'), row.get('County'), row.get('State'), 
            "United States", "-", row.get('Coordinates')
        )

        # Subject
        get_or_create_member(
            cursor, f_name, l_name, current_gen, 
            gender='Male', race=row.get('Race'), ethnicity=row.get('Ethnicity'),
            birth_date=format_date(row.get('Gen_1_Birth_Date')), 
            birth_loc_id=loc_id, father_id=fid, mother_id=mid, alias=row.get('Alias')
        )

# --- PHASE 2: DIRECTORY MIGRATION ---

def run_directory_migration(cursor):
    print("--- Phase 2: Migrating revolutionary_wars_directory ---")
    cursor.execute("SELECT * FROM public.revolutionary_wars_directory")
    records = cursor.fetchall()
    
    for row in records:
        # 1. Process Locations (Multiple points mapped to separate records if they exist)
        # Main Residence
        base_loc_id = get_or_create_location(
            cursor, row['city'], row['county'], row['state'], 
            row['country'], row['landmark'], row['areas_for_coordinates']
        )
        # Arrival Port
        get_or_create_location(
            cursor, row['arrival_port'], None, None, row['arrival_port_country'], "-", row['arrival_coordinates']
        )
        # Departure Point
        get_or_create_location(
            cursor, None, None, None, None, "-", row['departure_coordinates']
        )

        # 2. Determine Generations & Hierarchies
        has_gm = bool(clean_val(row['grandmother_first_name']) or clean_val(row['grandmother_surname']))
        has_parents = bool(clean_val(row['father_first_name']) or clean_val(row['father_surname']) or 
                           clean_val(row['mother_first_name']) or clean_val(row['mother_surname']))
        
        gm_gen = 1 if has_gm else None
        p_gen = 2 if has_gm else (1 if has_parents else None)
        s_gen = 3 if has_gm else (2 if has_parents else 1)

        # 3. Create Ancestors
        gm_id = get_or_create_member(cursor, row['grandmother_first_name'], row['grandmother_surname'], gm_gen, gender='Female') if has_gm else None
        
        m_id = get_or_create_member(
            cursor, row['mother_first_name'], row['mother_surname'], p_gen, 
            gender='Female', mother_id=gm_id
        ) if (clean_val(row['mother_first_name']) or clean_val(row['mother_surname'])) else None
        
        f_id = get_or_create_member(
            cursor, row['father_first_name'], row['father_surname'], p_gen, gender='Male'
        ) if (clean_val(row['father_first_name']) or clean_val(row['father_surname'])) else None

        # 4. Create Soldier (Subject)
        # Mapping requested: last_name -> surname, '-' -> alias, birthdate -> birth_date
        get_or_create_member(
            cursor, 
            first_name=row['first_name'], 
            last_name=row['surname'], # row['surname'] from source mapped to 'surname' column
            alias='-', 
            generation=s_gen, 
            gender=row['gender'], 
            race=row['race'], 
            ethnicity=row['ethnicity'],
            birth_date=format_date(row['birthdate']), 
            birth_loc_id=base_loc_id, 
            father_id=f_id, 
            mother_id=m_id, 
            directory_id=row['id']
        )

# --- MAIN EXECUTION ---

def main():
    try:
        print("Connecting to database...")
        conn = psycopg2.connect(DB_URL)
        cursor = conn.cursor(cursor_factory=DictCursor)
        
        run_excel_ingestion(cursor)
        run_directory_migration(cursor)
        
        conn.commit()
        print("Full migration successful!")
    except Exception as e:
        if 'conn' in locals(): conn.rollback()
        print(f"Migration failed: {e}")
    finally:
        if 'conn' in locals(): conn.close()

if __name__ == "__main__":
    main()