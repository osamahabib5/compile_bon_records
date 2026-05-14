import os
import pandas as pd
import psycopg2
from psycopg2 import sql, extras
from dotenv import load_dotenv

load_dotenv()

# --- CONNECTION CONFIGURATION ---
DB_CONNECTION_STRING = os.getenv("DB_CONNECTION_STRING")
EXCEL_PATH =  "Consolidated_Book_of_Negroes_v12.xlsx"
COMMIT_EVERY = 10

def get_db_connection():
    if not DB_CONNECTION_STRING:
        print("Error: DB_CONNECTION_STRING not found in environment.")
        return None
    try:
        return psycopg2.connect(DB_CONNECTION_STRING)
    except Exception as e:
        print(f"Failed to connect to Database: {e}")
        return None

def ensure_table(conn):
    """Create locations_revised table and add columns to book_of_negroes if missing."""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS locations_revised (
                locations_id SERIAL PRIMARY KEY,
                city TEXT,
                county TEXT,
                state TEXT,
                coordinates TEXT,
                country TEXT,
                landmark TEXT,
                created_at TIMESTAMP DEFAULT NOW()
            );
        """)
        # Ensure foreign key columns exist in book_of_negroes
        for col in ["departure_location_id", "arrival_location_id"]:
            cur.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_name='book_of_negroes' AND column_name=%s
            """, (col,))
            if not cur.fetchone():
                cur.execute(sql.SQL("ALTER TABLE book_of_negroes ADD COLUMN {} INTEGER;")
                            .format(sql.Identifier(col)))
    conn.commit()

def get_or_create_location(conn, location_dict):
    """
    Return the locations_id of an existing location that matches all non-null fields,
    or insert a new row and return its id.
    """
    fields = ["city", "county", "state", "country", "landmark", "coordinates"]
    conditions = []
    values = []

    for f in fields:
        val = location_dict.get(f)
        # Treat None, empty strings, and "-" as NULL in the database
        if val is not None and str(val).strip() not in ["", "-"]:
            conditions.append(sql.Identifier(f) + sql.SQL(" = %s"))
            values.append(val)
        else:
            conditions.append(sql.Identifier(f) + sql.SQL(" IS NULL"))

    where_clause = sql.SQL(" AND ").join(conditions)

    with conn.cursor() as cur:
        # Check if location already exists
        cur.execute(
            sql.SQL("SELECT locations_id FROM locations_revised WHERE {}").format(where_clause),
            values
        )
        row = cur.fetchone()
        if row:
            return row[0]

        # Build lists of columns and values to insert (non-null only)
        insert_cols = []
        insert_vals = []
        for f in fields:
            val = location_dict.get(f)
            if val is not None and str(val).strip() not in ["", "-"]:
                insert_cols.append(sql.Identifier(f))
                insert_vals.append(val)

        # If every field is null/empty, insert a fully-null row via DEFAULT VALUES
        if not insert_cols:
            cur.execute(
                "INSERT INTO locations_revised DEFAULT VALUES RETURNING locations_id"
            )
        else:
            cur.execute(
                sql.SQL("INSERT INTO locations_revised ({}) VALUES ({}) RETURNING locations_id").format(
                    sql.SQL(", ").join(insert_cols),
                    sql.SQL(", ").join([sql.Placeholder()] * len(insert_vals))
                ),
                insert_vals
            )

        return cur.fetchone()[0]

def update_bon_row(conn, notes, dep_id, arr_id):
    """Update book_of_negroes departure_location_id and arrival_location_id for given notes."""
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE book_of_negroes
            SET departure_location_id = %s,
                arrival_location_id = %s
            WHERE TRIM(notes) = %s
        """, (dep_id, arr_id, notes))

def main():
    conn = get_db_connection()
    if not conn:
        return

    try:
        ensure_table(conn)

        if not os.path.exists(EXCEL_PATH):
            print(f"Error: Excel file not found at {EXCEL_PATH}")
            return

        df = pd.read_excel(EXCEL_PATH, dtype=str)
        df = df.fillna("")

        required_cols = ["Notes", "City", "County", "State", "Country", "Landmark",
                         "Departure_Coordinates", "Arrival_Port", "Arrival_Port_Country",
                         "Arrival_Coordinates"]

        for col in required_cols:
            if col not in df.columns:
                raise ValueError(f"Missing required column in Excel: {col}")

        count = 0
        for _, row in df.iterrows():
            notes = str(row["Notes"]).strip()
            if not notes:
                continue

            # --- Departure location ---
            dep_loc = {
                "city": row.get("City"),
                "county": row.get("County"),
                "state": row.get("State"),
                "country": row.get("Country"),
                "landmark": row.get("Landmark"),
                "coordinates": row.get("Departure_Coordinates"),
            }
            dep_id = get_or_create_location(conn, dep_loc)

            # --- Arrival location ---
            arr_loc = {
                "city": row.get("Arrival_Port"),
                "county": None,
                "state": None,
                "country": row.get("Arrival_Port_Country"),
                "landmark": None,
                "coordinates": row.get("Arrival_Coordinates"),
            }
            arr_id = get_or_create_location(conn, arr_loc)

            # Update book_of_negroes
            update_bon_row(conn, notes, dep_id, arr_id)

            count += 1
            if count % COMMIT_EVERY == 0:
                conn.commit()
                print(f"Processed {count} rows...")

        conn.commit()
        print(f"Finished. Total rows processed: {count}")

    except Exception as e:
        print(f"An error occurred during processing: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    main()