import psycopg2
from psycopg2 import extras

# 1. Define your connection string
# Replace with your actual credentials
CONN_STRING = "postgresql://genealogy_user:Bl%40ckLiveSMaTTeR324.@sofafea-postgres.postgres.database.azure.com/postgres?sslmode=require"

def get_location_id(cursor, city, county, state, country, landmark, coords):
    """Upserts a location and returns its ID, following existing ingestion logic."""
    if not any([city, county, state, country, landmark, coords]):
        return None
    
    # Using ON CONFLICT to prevent duplicates based on your unique indexes
    query = """
    INSERT INTO locations (city, county, state, country, landmark, coordinates)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (city, county, state, country, landmark, coordinates) DO UPDATE 
    SET city = EXCLUDED.city 
    RETURNING location_id;
    """
    cursor.execute(query, (city, county, state, country, landmark, coords))
    result = cursor.fetchone()
    return result[0] if result else None

def transfer_directory_data():
    try:
        # 2. Use the connection string to connect
        conn = psycopg2.connect(CONN_STRING)
        cur = conn.cursor(cursor_factory=extras.DictCursor)
        
        # Fetch data from the source directory table
        cur.execute("SELECT * FROM revolutionary_wars_directory")
        records = cur.fetchall()

        print(f"Starting transfer of {len(records)} records...")

        for rec in records:
            # --- STEP 1: Process Locations ---
            # Mapping city, county, state, country, landmark, and coordinates
            loc_id = get_location_id(
                cur, rec['city'], rec['county'], rec['state'], 
                rec['country'], rec['landmark'], rec['departure_coordinates']
            )

            # --- STEP 2: Process Generation 2 (Father & Mother) ---
            # We create these first so we can get their IDs to link to the soldier
            father_id = None
            if rec['father_first_name'] or rec['father_surname']:
                cur.execute("""
                    INSERT INTO family_members (first_name, last_name, gender, generation_number, directory_id)
                    VALUES (%s, %s, 'Male', 2, %s)
                    RETURNING member_id
                """, (rec['father_first_name'], rec['father_surname'], rec['id']))
                father_id = cur.fetchone()[0]

            mother_id = None
            if rec['mother_first_name'] or rec['mother_surname']:
                cur.execute("""
                    INSERT INTO family_members (first_name, last_name, gender, generation_number, directory_id)
                    VALUES (%s, %s, 'Female', 2, %s)
                    RETURNING member_id
                """, (rec['mother_first_name'], rec['mother_surname'], rec['id']))
                mother_id = cur.fetchone()[0]

            # --- STEP 3: Process Generation 3 (Grandmother) ---
            if rec['grandmother_first_name'] or rec['grandmother_surname']:
                cur.execute("""
                    INSERT INTO family_members (first_name, last_name, gender, generation_number, directory_id)
                    VALUES (%s, %s, 'Female', 3, %s)
                """, (rec['grandmother_first_name'], rec['grandmother_surname'], rec['id']))

            # --- STEP 4: Process Generation 1 (The Soldier) ---
            # Mapping columns as specified: first_name -> first_name, surname -> last_name, etc.
            cur.execute("""
                INSERT INTO family_members (
                    directory_id, first_name, last_name, alias, gender, 
                    race, ethnicity, birth_date, birth_location_id, 
                    generation_number, father_id, mother_id
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 1, %s, %s)
            """, (
                rec['id'], rec['first_name'], rec['surname'], '-', rec['gender'],
                rec['race'], rec['ethnicity'], rec['birthdate'], loc_id,
                father_id, mother_id
            ))

        conn.commit()
        print("Data transfer complete.")

    except Exception as e:
        if 'conn' in locals(): conn.rollback()
        print(f"Error: {e}")
    finally:
        if 'cur' in locals(): cur.close()
        if 'conn' in locals(): conn.close()

if __name__ == "__main__":
    transfer_directory_data()