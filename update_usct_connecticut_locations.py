import os

import pandas as pd
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv

load_dotenv()

DB_CONNECTION_STRING = os.getenv("DB_CONNECTION_STRING")
EXCEL_PATH = "USCTs_Connecticut_rev_05.xlsx"
COMMIT_EVERY = 10


def clean_value(value):
    """Normalize spreadsheet values so blanks map cleanly to SQL NULL."""
    if value is None or pd.isna(value):
        return None
    cleaned = str(value).strip()
    return None if cleaned.lower() in {"", "nan", "-"} else cleaned


def get_db_connection():
    if not DB_CONNECTION_STRING:
        print("Error: DB_CONNECTION_STRING not found in environment.")
        return None
    try:
        return psycopg2.connect(DB_CONNECTION_STRING)
    except Exception as exc:
        print(f"Failed to connect to database: {exc}")
        return None


def ensure_schema(conn):
    """Ensure the destination table, columns, and foreign keys point to locations_revised."""
    with conn.cursor() as cur:
        cur.execute(
            """
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
            """
        )

        for col in ["pob_location_id", "residence_location_id", "enlistment_location_id"]:
            cur.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'usct_connecticut' AND column_name = %s
                """,
                (col,),
            )
            if not cur.fetchone():
                cur.execute(
                    sql.SQL("ALTER TABLE usct_connecticut ADD COLUMN {} INTEGER").format(
                        sql.Identifier(col)
                    )
                )

        fk_columns = [
            "pob_location_id",
            "residence_location_id",
            "enlistment_location_id",
        ]
        for col in fk_columns:
            cur.execute(
                """
                SELECT con.conname, ref.relname
                FROM pg_constraint con
                JOIN pg_class tbl ON tbl.oid = con.conrelid
                JOIN pg_class ref ON ref.oid = con.confrelid
                JOIN unnest(con.conkey) AS cols(attnum) ON TRUE
                JOIN pg_attribute att
                  ON att.attrelid = tbl.oid
                 AND att.attnum = cols.attnum
                WHERE con.contype = 'f'
                  AND tbl.relname = 'usct_connecticut'
                  AND att.attname = %s
                """,
                (col,),
            )
            existing_fk = cur.fetchone()
            expected_constraint = f"usct_connecticut_{col}_fkey"

            if existing_fk and existing_fk[1] != "locations_revised":
                cur.execute(
                    sql.SQL("ALTER TABLE usct_connecticut DROP CONSTRAINT {}").format(
                        sql.Identifier(existing_fk[0])
                    )
                )
                existing_fk = None

            if not existing_fk:
                cur.execute(
                    sql.SQL(
                        """
                        ALTER TABLE usct_connecticut
                        ADD CONSTRAINT {} FOREIGN KEY ({})
                        REFERENCES locations_revised (locations_id)
                        """
                    ).format(
                        sql.Identifier(expected_constraint),
                        sql.Identifier(col),
                    )
                )
    conn.commit()


def build_location(row, prefix, coordinate_column):
    return {
        "city": clean_value(row.get(f"{prefix}_City")),
        "county": clean_value(row.get(f"{prefix}_County")),
        "state": clean_value(row.get(f"{prefix}_State")),
        "country": clean_value(row.get(f"{prefix}_Country")),
        "coordinates": clean_value(row.get(coordinate_column)),
        "landmark": None,
    }


def get_or_create_location(conn, location_dict, cache):
    """
    Return the existing location id for an exact nullable match,
    or insert a new row in locations_revised.
    """
    fields = ["city", "county", "state", "country", "landmark", "coordinates"]
    normalized = {field: clean_value(location_dict.get(field)) for field in fields}

    # Avoid creating duplicate fully-empty placeholder rows.
    if not any(normalized.values()):
        return None

    cache_key = tuple(normalized[field] for field in fields)
    if cache_key in cache:
        return cache[cache_key]

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT locations_id
            FROM locations_revised
            WHERE city IS NOT DISTINCT FROM %s
              AND county IS NOT DISTINCT FROM %s
              AND state IS NOT DISTINCT FROM %s
              AND country IS NOT DISTINCT FROM %s
              AND landmark IS NOT DISTINCT FROM %s
              AND coordinates IS NOT DISTINCT FROM %s
            """,
            tuple(normalized[field] for field in fields),
        )
        row = cur.fetchone()
        if row:
            cache[cache_key] = row[0]
            return row[0]

        insert_cols = []
        insert_vals = []
        for field in fields:
            value = normalized[field]
            if value is not None:
                insert_cols.append(sql.Identifier(field))
                insert_vals.append(value)

        cur.execute(
            sql.SQL(
                "INSERT INTO locations_revised ({}) VALUES ({}) RETURNING locations_id"
            ).format(
                sql.SQL(", ").join(insert_cols),
                sql.SQL(", ").join([sql.Placeholder()] * len(insert_vals)),
            ),
            insert_vals,
        )
        location_id = cur.fetchone()[0]
        cache[cache_key] = location_id
        return location_id


def update_usct_row(conn, excel_id, pob_id, residence_id, enlistment_id):
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE usct_connecticut
            SET pob_location_id = %s,
                residence_location_id = %s,
                enlistment_location_id = %s
            WHERE TRIM(excel_id) = %s
            """,
            (pob_id, residence_id, enlistment_id, excel_id),
        )
        return cur.rowcount


def main():
    conn = get_db_connection()
    if not conn:
        return

    try:
        ensure_schema(conn)

        if not os.path.exists(EXCEL_PATH):
            print(f"Error: Excel file not found at {EXCEL_PATH}")
            return

        df = pd.read_excel(EXCEL_PATH, dtype=str).fillna("")
        required_cols = [
            "ID",
            "Enlistment_City",
            "Enlistment_County",
            "Enlistment_State",
            "Enlistment_Country",
            "Enlistment_Coordinates",
            "Residence_City",
            "Residence_County",
            "Residence_State",
            "Residence_Country",
            "Residence_coordinates",
            "POB_City",
            "POB_County",
            "POB_State",
            "POB_Country",
            "Birth_coordinates",
        ]
        missing = [col for col in required_cols if col not in df.columns]
        if missing:
            raise ValueError(f"Missing required Excel columns: {', '.join(missing)}")

        count = 0
        updated = 0
        missing_targets = 0
        location_cache = {}

        for _, row in df.iterrows():
            excel_id = clean_value(row.get("ID"))
            if not excel_id:
                continue

            enlistment_loc = build_location(row, "Enlistment", "Enlistment_Coordinates")
            residence_loc = build_location(row, "Residence", "Residence_coordinates")
            pob_loc = build_location(row, "POB", "Birth_coordinates")

            enlistment_id = get_or_create_location(conn, enlistment_loc, location_cache)
            residence_id = get_or_create_location(conn, residence_loc, location_cache)
            pob_id = get_or_create_location(conn, pob_loc, location_cache)

            rowcount = update_usct_row(conn, excel_id, pob_id, residence_id, enlistment_id)
            if rowcount == 0:
                missing_targets += 1
                print(f"No usct_connecticut row found for excel_id={excel_id}")
            else:
                updated += rowcount

            count += 1
            if count % COMMIT_EVERY == 0:
                conn.commit()
                print(
                    f"Processed {count} rows... updated={updated}, "
                    f"missing_targets={missing_targets}"
                )

        conn.commit()
        print(
            "Finished. "
            f"Total rows processed: {count}; "
            f"rows updated: {updated}; "
            f"missing target rows: {missing_targets}; "
            f"unique cached locations: {len(location_cache)}"
        )
    except Exception as exc:
        print(f"An error occurred: {exc}")
        conn.rollback()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
