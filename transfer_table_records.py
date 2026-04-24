import psycopg2
from psycopg2.extras import DictCursor
import os

# Define the connection string (DSN)
# Example Azure format: postgresql://[user]:[password]@[host]:5432/[dbname]?sslmode=require
DB_URL = os.environ.get(
    "DATABASE_URL", 
    "postgresql://genealogy_user:Bl%40ckLiveSMaTTeR324.@sofafea-postgres.postgres.database.azure.com/postgres?sslmode=require"
)

def get_or_create_location(cursor, city, county, state, country, landmark, coordinates):
    """
    Inserts a location and returns its ID. Uses ON CONFLICT to avoid duplicate errors.
    """
    if not any([city, county, state, country, landmark, coordinates]):
        return None

    query = """
        INSERT INTO public.locations (city, county, state, country, landmark, coordinates)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (city, county, state, country, landmark, coordinates) DO UPDATE 
        SET city = EXCLUDED.city
        RETURNING location_id;
    """
    cursor.execute(query, (city, county, state, country, landmark, coordinates))
    result = cursor.fetchone()
    
    if result:
        return result['location_id']
    else:
        cursor.execute("""
            SELECT location_id FROM public.locations 
            WHERE city IS NOT DISTINCT FROM %s AND county IS NOT DISTINCT FROM %s 
            AND state IS NOT DISTINCT FROM %s AND country IS NOT DISTINCT FROM %s
            AND landmark IS NOT DISTINCT FROM %s AND coordinates IS NOT DISTINCT FROM %s
        """, (city, county, state, country, landmark, coordinates))
        return cursor.fetchone()['location_id']

def insert_family_member(cursor, directory_id, first_name, last_name, gender, race, ethnicity, 
                         generation, father_id=None, mother_id=None, birth_location_id=None, war=None):
    """
    Inserts a family member and returns their generated member_id.
    """
    if not first_name and not last_name:
        return None

    query = """
        INSERT INTO public.family_members (
            directory_id, first_name, last_name, gender, race, ethnicity, 
            generation_number, father_id, mother_id, birth_location_id, war
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        ) RETURNING member_id;
    """
    cursor.execute(query, (
        directory_id, first_name, last_name, gender, race, ethnicity, 
        generation, father_id, mother_id, birth_location_id, war
    ))
    return cursor.fetchone()['member_id']

def run_migration():
    # Connect using the single connection string
    print(f"Connecting to database...")
    try:
        conn = psycopg2.connect(DB_URL)
        cursor = conn.cursor(cursor_factory=DictCursor)
    except Exception as e:
        print(f"Failed to connect to the database: {e}")
        return
    
    try:
        cursor.execute("SELECT * FROM public.revolutionary_wars_directory")
        records = cursor.fetchall()
        print(f"Found {len(records)} records to process.")
        
        for row in records:
            # 1. Process Location
            loc_id = get_or_create_location(
                cursor, 
                row['city'], row['county'], row['state'], 
                row['country'], row['landmark'], row['areas_for_coordinates']
            )
            
            # 2. Determine Generations
            has_grandmother = bool(row['grandmother_first_name'] or row['grandmother_surname'])
            has_parents = bool(row['father_first_name'] or row['father_surname'] or 
                               row['mother_first_name'] or row['mother_surname'])
            
            gm_gen = 1 if has_grandmother else None
            parent_gen = 2 if has_grandmother else (1 if has_parents else None)
            soldier_gen = 3 if has_grandmother else (2 if has_parents else 1)
            
            # 3. Insert Grandmother
            gm_id = None
            if has_grandmother:
                gm_id = insert_family_member(
                    cursor=cursor,
                    directory_id=row['id'],
                    first_name=row['grandmother_first_name'],
                    last_name=row['grandmother_surname'],
                    gender='Female',
                    race=row['race'],
                    ethnicity=row['ethnicity'],
                    generation=gm_gen
                )
            
            # 4. Insert Parents
            mother_id = None
            if row['mother_first_name'] or row['mother_surname']:
                mother_id = insert_family_member(
                    cursor=cursor,
                    directory_id=row['id'],
                    first_name=row['mother_first_name'],
                    last_name=row['mother_surname'],
                    gender='Female',
                    race=row['race'],
                    ethnicity=row['ethnicity'],
                    generation=parent_gen,
                    mother_id=gm_id
                )
                
            father_id = None
            if row['father_first_name'] or row['father_surname']:
                father_id = insert_family_member(
                    cursor=cursor,
                    directory_id=row['id'],
                    first_name=row['father_first_name'],
                    last_name=row['father_surname'],
                    gender='Male',
                    race=row['race'],
                    ethnicity=row['ethnicity'],
                    generation=parent_gen
                )
            
            # 5. Insert Soldier
            soldier_id = insert_family_member(
                cursor=cursor,
                directory_id=row['id'],
                first_name=row['first_name'],
                last_name=row['surname'],
                gender=row['gender'],
                race=row['race'],
                ethnicity=row['ethnicity'],
                generation=soldier_gen,
                father_id=father_id,
                mother_id=mother_id,
                birth_location_id=loc_id,
                war='Revolutionary War'
            )
            
        conn.commit()
        print(f"Successfully migrated {len(records)} records and their hierarchies.")

    except Exception as e:
        conn.rollback()
        print(f"An error occurred during migration: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    run_migration()