import os
import pandas as pd
import psycopg2
from dotenv import load_dotenv

# Load environment variables from a .env file if present
load_dotenv()

def populate_departure_locations(excel_path):
    # 1. Get database connection string from the environment variable
    db_url = os.getenv("DB_CONNECTION_STRING")
    if not db_url:
        print("Error: DB_CONNECTION_STRING environment variable not set.")
        return

    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(db_url)
        cursor = conn.cursor()
        print("Connected to the database successfully.")

        # 2. Load the Excel file
        df_excel = pd.read_excel(excel_path)
        
        # Standardize 'Notes' for comparison
        df_excel['Notes'] = df_excel['Notes'].astype(str).str.strip()

        # Geographic columns to check
        geo_columns = ['City', 'County', 'State', 'Landmark', 'Country']

        for index, row in df_excel.iterrows():
            excel_notes = row['Notes']
            
            # 3. Dynamically build the query based on populated Excel columns
            # '-' is treated as a requirement to match a NULL value in the database
            active_filters = {}
            for col in geo_columns:
                val = row[col]
                # Check if cell is populated and not NaN
                if pd.notna(val):
                    val_str = str(val).strip()
                    # If value is '-', it maps to None (NULL in SQL)
                    active_filters[col.lower()] = None if val_str == '-' else val_str

            if not active_filters:
                print(f"Skipping row {index}: No geographic data provided.")
                continue

            # Build the WHERE clause dynamically
            # Handles both standard values (= %s) and NULL values (IS NULL)
            conditions = []
            params = []
            for col, val in active_filters.items():
                if val is None:
                    conditions.append(f"{col} IS NULL")
                else:
                    conditions.append(f"{col} = %s")
                    params.append(val)

            where_clause = " AND ".join(conditions)

            find_loc_query = f"""
                SELECT location_id 
                FROM public.locations 
                WHERE {where_clause}
                LIMIT 1;
            """
            
            cursor.execute(find_loc_query, params)
            loc_result = cursor.fetchone()

            if loc_result:
                location_id = loc_result[0]

                # 4. Update the book_of_negroes table
                update_query = """
                    UPDATE public.book_of_negroes
                    SET departure_location_id = %s
                    WHERE TRIM(notes) = %s;
                """
                
                cursor.execute(update_query, (location_id, excel_notes))
                
                if cursor.rowcount > 0:
                    print(f"Updated: Note matching '{excel_notes[:30]}...' -> Location ID: {location_id}")
                else:
                    print(f"No match in book_of_negroes for Note: '{excel_notes[:30]}...'")
            else:
                filter_desc = ", ".join([f"{k}: {v if v is not None else 'NULL'}" for k, v in active_filters.items()])
                print(f"Location not found in database for filters: {filter_desc}")

        # Commit all changes
        conn.commit()
        print("Database update complete.")

    except Exception as e:
        print(f"An error occurred: {e}")
        if conn:
            conn.rollback()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    # Ensure your Excel file path is correct
    PATH_TO_EXCEL = "Consolidated_Book_of_Negroes_v11.xlsx" 
    populate_departure_locations(PATH_TO_EXCEL)