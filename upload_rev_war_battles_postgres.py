import os

import pandas as pd
import psycopg2
from dotenv import load_dotenv


load_dotenv()


DB_CONNECTION_STRING = os.getenv("DB_CONNECTION_STRING")
INPUT_FILE = "Rev_War_Battles_v03.xlsx"
DEFAULT_COUNTRY = "United States"
DEFAULT_LANDMARK = "-"
LOCATION_COLUMNS = ["City", "County", "State", "Landmark", "Country", "Coordinates"]


def clean_val(value):
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    if pd.isna(value):
        return None

    cleaned = str(value).strip()
    if not cleaned or cleaned.lower() in {"nan", "none"} or cleaned == "-":
        return None
    return cleaned


def get_db_connection():
    if not DB_CONNECTION_STRING:
        print("DB_CONNECTION_STRING not found in the environment.")
        return None

    try:
        return psycopg2.connect(DB_CONNECTION_STRING)
    except Exception as exc:
        print(f"Failed to connect to Azure PostgreSQL: {exc}")
        return None


def create_rev_war_details_table(cur):
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS public.rev_war_details (
            rev_war_detail_id SERIAL PRIMARY KEY,
            location_id INTEGER REFERENCES public.locations(location_id),
            date TEXT,
            theater TEXT,
            area TEXT,
            engagement TEXT,
            type TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_rev_war_details_location_id
        ON public.rev_war_details(location_id)
        """
    )


def get_or_insert_location(cur, city, county, state, landmark, country, coordinates):
    city = clean_val(city)
    county = clean_val(county)
    state = clean_val(state)
    landmark = clean_val(landmark)
    country = clean_val(country)
    coordinates = clean_val(coordinates)

    if not any([city, county, state, landmark, country, coordinates]):
        return None

    # First try an exact logical match across the location-identifying fields
    # requested by the workbook mapping. Excel "-" values have already been
    # normalized to NULL by clean_val().
    cur.execute(
        """
        SELECT location_id
        FROM public.locations
        WHERE city IS NOT DISTINCT FROM %s
          AND county IS NOT DISTINCT FROM %s
          AND state IS NOT DISTINCT FROM %s
          AND landmark IS NOT DISTINCT FROM %s
          AND country IS NOT DISTINCT FROM %s
          AND coordinates IS NOT DISTINCT FROM %s
        """,
        (city, county, state, landmark, country, coordinates),
    )
    existing = cur.fetchone()
    if existing:
        return existing[0]

    # The database has a uniqueness constraint on (city, county, state), so if
    # that location core already exists, reuse it even if country/landmark were
    # stored differently or left NULL in the original row.
    cur.execute(
        """
        SELECT location_id
        FROM public.locations
        WHERE city IS NOT DISTINCT FROM %s
          AND county IS NOT DISTINCT FROM %s
          AND state IS NOT DISTINCT FROM %s
        """,
        (city, county, state),
    )
    existing_core = cur.fetchone()
    if existing_core:
        return existing_core[0]

    cur.execute("SAVEPOINT location_insert_sp")
    try:
        cur.execute(
            """
            INSERT INTO public.locations (city, county, state, coordinates, country, landmark)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING location_id
            """,
            (
                city,
                county,
                state,
                coordinates,
                country or DEFAULT_COUNTRY,
                landmark or DEFAULT_LANDMARK,
            ),
        )
        inserted_id = cur.fetchone()[0]
        cur.execute("RELEASE SAVEPOINT location_insert_sp")
        return inserted_id
    except psycopg2.errors.UniqueViolation:
        cur.execute("ROLLBACK TO SAVEPOINT location_insert_sp")
        cur.execute("RELEASE SAVEPOINT location_insert_sp")
        cur.execute(
            """
            SELECT location_id
            FROM public.locations
            WHERE city IS NOT DISTINCT FROM %s
              AND county IS NOT DISTINCT FROM %s
              AND state IS NOT DISTINCT FROM %s
            """,
            (city, county, state),
        )
        existing_after_conflict = cur.fetchone()
        if existing_after_conflict:
            return existing_after_conflict[0]
        raise


def insert_rev_war_detail(cur, row, location_id):
    cur.execute(
        """
        INSERT INTO public.rev_war_details (
            location_id,
            date,
            theater,
            area,
            engagement,
            type
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (
            location_id,
            clean_val(row.get("Date")),
            clean_val(row.get("Theater")),
            clean_val(row.get("Area")),
            clean_val(row.get("Engagement")),
            clean_val(row.get("Type")),
        ),
    )


def validate_columns(df):
    required = ["Date", "Theater", "Area", "Engagement", "Type"] + LOCATION_COLUMNS
    missing = [column for column in required if column not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")


def run_upload(file_path):
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        return

    df = pd.read_excel(file_path)
    df.columns = df.columns.astype(str).str.strip()
    validate_columns(df)

    conn = get_db_connection()
    if not conn:
        return

    cur = conn.cursor()

    try:
        create_rev_war_details_table(cur)

        for idx, row in df.iterrows():
            location_id = get_or_insert_location(
                cur,
                row.get("City"),
                row.get("County"),
                row.get("State"),
                row.get("Landmark"),
                row.get("Country"),
                row.get("Coordinates"),
            )

            insert_rev_war_detail(cur, row, location_id)

            if (idx + 1) % 25 == 0:
                print(f"Processed {idx + 1} rows...")

        conn.commit()
        print("Upload complete.")
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    run_upload(INPUT_FILE)
