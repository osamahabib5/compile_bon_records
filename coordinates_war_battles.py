import os
import re
from typing import Dict, Optional

import pandas as pd
import spacy
from geopy.extra.rate_limiter import RateLimiter
from geopy.geocoders import Nominatim


INPUT_FILE = "Rev_War_Battles_v03.xlsx"
OUTPUT_FILE = "Rev_War_Battles_v03.xlsx"
AREA_COLUMN = "Area"
AREA_FALLBACK_COLUMNS = ["State"]
TARGET_COLUMNS = [
    "Date",
    "Theater",
    "Area",
    "City",
    "County",
    "State",
    "Landmark",
    "Country",
    "Coordinates",
    "Engagement",
    "Type",
]
EMPTY_VALUE = "-"


LANDMARK_KEYWORDS = {
    "island",
    "islands",
    "coast",
    "mount",
    "mountain",
    "mountains",
    "fort",
    "bay",
    "gulf",
    "sea",
    "ocean",
    "harbor",
    "harbour",
    "strait",
    "sound",
    "cape",
    "peninsula",
    "ridge",
    "valley",
    "point",
    "hill",
    "hills",
    "lake",
    "river",
    "falls",
    "beach",
    "head",
    "banks",
    "pass",
    "heights",
}


def clean_val(value):
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    if pd.isna(value):
        return None

    cleaned = str(value).strip()
    if not cleaned or cleaned.lower() in {"nan", "none"}:
        return None
    return cleaned


def normalize_area(value):
    cleaned = clean_val(value)
    if not cleaned:
        return None
    return re.sub(r"\s+", " ", cleaned).strip()


def as_output_value(value):
    cleaned = clean_val(value)
    return cleaned if cleaned else EMPTY_VALUE


def looks_like_landmark(text: str) -> bool:
    lowered = text.lower()
    return any(keyword in lowered for keyword in LANDMARK_KEYWORDS)


def values_match(left, right) -> bool:
    left_clean = normalize_area(left)
    right_clean = normalize_area(right)
    if not left_clean or not right_clean:
        return False
    return left_clean.lower() == right_clean.lower()


def choose_area_column(df: pd.DataFrame) -> str:
    if AREA_COLUMN in df.columns:
        return AREA_COLUMN

    for fallback in AREA_FALLBACK_COLUMNS:
        if fallback in df.columns:
            print(f"Using '{fallback}' as the source area column because '{AREA_COLUMN}' is missing.")
            return fallback

    raise ValueError(
        f"Missing '{AREA_COLUMN}' column and no fallback area column found. "
        f"Available columns: {list(df.columns)}"
    )


def ensure_output_columns(df: pd.DataFrame, source_area_column: str) -> pd.DataFrame:
    working_df = df.copy()

    if "Area" not in working_df.columns:
        insert_after = working_df.columns.get_loc(source_area_column)
        working_df.insert(insert_after + 1, "Area", working_df[source_area_column])
    else:
        working_df["Area"] = working_df["Area"].where(working_df["Area"].notna(), working_df[source_area_column])

    for column in TARGET_COLUMNS:
        if column not in working_df.columns:
            working_df[column] = None

    # These columns may be read as float64 when they are mostly empty.
    # Cast them up front so later string assignments from geocoding do not fail.
    editable_text_columns = [
        "Area",
        "City",
        "County",
        "State",
        "Landmark",
        "Country",
        "Coordinates",
        "Engagement",
        "Type",
    ]
    for column in editable_text_columns:
        working_df[column] = working_df[column].astype("object")

    return working_df


def build_query(place_text: str) -> str:
    return normalize_area(place_text)


def load_ner_model():
    try:
        return spacy.load("en_core_web_md")
    except OSError as exc:
        raise RuntimeError(
            "spaCy model 'en_core_web_md' is not installed. "
            "Install it with: python -m spacy download en_core_web_md"
        ) from exc


def classify_location(area_text: str, location, nlp) -> Dict[str, Optional[str]]:
    address = location.raw.get("address", {}) if location else {}
    area_text = normalize_area(area_text)
    doc = nlp(area_text)
    entities = {ent.label_ for ent in doc.ents}

    city = (
        address.get("city")
        or address.get("town")
        or address.get("village")
        or address.get("hamlet")
        or address.get("municipality")
        or address.get("borough")
        or address.get("suburb")
    )
    county = address.get("county") or address.get("state_district")
    state = address.get("state") or address.get("region")
    country = address.get("country")

    location_type = (location.raw.get("type") or "").lower()
    addresstype = (location.raw.get("addresstype") or "").lower()
    display_name = clean_val(location.raw.get("display_name")) or area_text

    landmark = None

    if looks_like_landmark(area_text):
        landmark = area_text
    elif location_type in {
        "island",
        "archipelago",
        "peak",
        "volcano",
        "mountain_range",
        "bay",
        "cape",
        "strait",
        "sea",
        "ocean",
        "peninsula",
        "reef",
        "beach",
        "coastline",
        "canal",
        "fort",
    }:
        landmark = area_text
    elif addresstype in {"island", "archipelago", "bay", "sea", "ocean", "peak"}:
        landmark = area_text
    elif "LOC" in entities and not city and not county and not state:
        landmark = area_text

    if country and area_text.lower() == country.lower():
        city = None
        county = None
        state = None
        landmark = None
    elif state and area_text.lower() == state.lower() and not city and not county:
        landmark = None
    elif city and area_text.lower() == city.lower():
        pass
    elif landmark:
        city = None
        county = None
        state = None if not state or state.lower() == area_text.lower() else state
    elif not city and not county and not state and country:
        landmark = area_text

    classified = {
        "validated": True,
        "area": area_text,
        "city": city,
        "county": county,
        "state": state,
        "landmark": landmark,
        "country": country,
        "coordinates": f"{location.latitude}, {location.longitude}",
        "display_name": display_name,
        "location_type": location_type or addresstype or None,
    }

    return enforce_area_membership(classified)


def enforce_area_membership(result: Dict[str, Optional[str]]) -> Dict[str, Optional[str]]:
    area_text = normalize_area(result.get("area"))
    if not area_text:
        return result

    if values_match(area_text, result.get("country")):
        result["city"] = None
        result["county"] = None
        result["state"] = None
        result["landmark"] = None
        result["country"] = area_text
        return result

    for key in ["city", "county", "state", "landmark", "country"]:
        if values_match(area_text, result.get(key)):
            return result

    if looks_like_landmark(area_text):
        result["landmark"] = area_text
        return result

    if result.get("country") and not result.get("city") and not result.get("county") and not result.get("state"):
        result["landmark"] = area_text
        return result

    if result.get("state") and not result.get("city") and not result.get("county"):
        result["state"] = area_text
        return result

    if result.get("county") and "county" in area_text.lower():
        result["county"] = area_text
        return result

    if result.get("city"):
        result["city"] = area_text
        return result

    if result.get("country"):
        result["country"] = area_text
        return result

    result["landmark"] = area_text
    return result


def geocode_and_classify_unique_areas(unique_areas, nlp):
    geolocator = Nominatim(user_agent="war_battles_geocoder", timeout=10)
    geocode = RateLimiter(
        geolocator.geocode,
        min_delay_seconds=1.2,
        max_retries=2,
        error_wait_seconds=5.0,
        swallow_exceptions=True,
    )

    results = {}
    total = len(unique_areas)
    print(f"Validating and classifying {total} unique area values...")

    for idx, area in enumerate(unique_areas, start=1):
        query = build_query(area)
        location = geocode(query, addressdetails=True)

        if location:
            results[area] = classify_location(area, location, nlp)
        else:
            results[area] = {
                "validated": False,
                "area": area,
                "city": None,
                "county": None,
                "state": None,
                "landmark": None,
                "country": None,
                "coordinates": None,
                "display_name": None,
                "location_type": None,
            }

        if idx == total or idx % 10 == 0:
            print(f"Processed {idx}/{total} unique areas")

    return results


def build_coordinate_query(row) -> Optional[str]:
    parts = [
        clean_val(row.get("City")),
        clean_val(row.get("County")),
        clean_val(row.get("State")),
        clean_val(row.get("Landmark")),
        clean_val(row.get("Country")),
    ]
    parts = [part for part in parts if part and part != EMPTY_VALUE]
    if not parts:
        return None
    return ", ".join(parts)


def fill_coordinates_from_parts(df: pd.DataFrame):
    geolocator = Nominatim(user_agent="war_battles_coordinate_builder", timeout=10)
    geocode = RateLimiter(
        geolocator.geocode,
        min_delay_seconds=1.2,
        max_retries=2,
        error_wait_seconds=5.0,
        swallow_exceptions=True,
    )

    coord_cache = {}

    for idx, row in df.iterrows():
        if clean_val(row.get("Coordinates")):
            continue

        query = build_coordinate_query(row)
        if not query:
            continue

        if query not in coord_cache:
            location = geocode(query, addressdetails=True)
            coord_cache[query] = f"{location.latitude}, {location.longitude}" if location else None

        if coord_cache[query]:
            df.at[idx, "Coordinates"] = coord_cache[query]

    return df


def preserve_existing_row_values(df: pd.DataFrame, idx: int, result: Dict[str, Optional[str]]):
    for column_name, result_key in [
        ("City", "city"),
        ("County", "county"),
        ("State", "state"),
        ("Landmark", "landmark"),
        ("Country", "country"),
    ]:
        existing_value = clean_val(df.at[idx, column_name]) if column_name in df.columns else None
        if existing_value and existing_value != EMPTY_VALUE:
            result[result_key] = existing_value

    existing_coordinates = clean_val(df.at[idx, "Coordinates"]) if "Coordinates" in df.columns else None
    if existing_coordinates:
        result["coordinates"] = existing_coordinates

    return result


def apply_area_results(df: pd.DataFrame, area_column: str, area_results: Dict[str, Dict[str, Optional[str]]]):
    for idx, row in df.iterrows():
        area_value = normalize_area(row.get(area_column))
        if not area_value:
            continue

        result = area_results.get(area_value)
        if not result:
            continue

        result = preserve_existing_row_values(df, idx, dict(result))
        result = enforce_area_membership(result)

        df.at[idx, "Area"] = area_value

        if result["validated"]:
            df.at[idx, "City"] = as_output_value(result["city"])
            df.at[idx, "County"] = as_output_value(result["county"])
            df.at[idx, "State"] = as_output_value(result["state"])
            df.at[idx, "Landmark"] = as_output_value(result["landmark"])
            df.at[idx, "Country"] = as_output_value(result["country"])
            if not clean_val(row.get("Coordinates")) and result["coordinates"]:
                df.at[idx, "Coordinates"] = result["coordinates"]
        else:
            df.at[idx, "City"] = EMPTY_VALUE
            df.at[idx, "County"] = EMPTY_VALUE
            df.at[idx, "State"] = EMPTY_VALUE
            df.at[idx, "Landmark"] = area_value
            df.at[idx, "Country"] = EMPTY_VALUE
            if not clean_val(row.get("Coordinates")):
                df.at[idx, "Coordinates"] = None

    return df


def process_sheet(df: pd.DataFrame, nlp) -> pd.DataFrame:
    source_area_column = choose_area_column(df)
    working_df = ensure_output_columns(df, source_area_column)

    unique_areas = sorted(
        {
            normalize_area(value)
            for value in working_df[source_area_column].tolist()
            if normalize_area(value)
        }
    )

    area_results = geocode_and_classify_unique_areas(unique_areas, nlp)
    working_df = apply_area_results(working_df, source_area_column, area_results)
    working_df = fill_coordinates_from_parts(working_df)

    return working_df[TARGET_COLUMNS]


def main():
    if not os.path.exists(INPUT_FILE):
        print(f"File not found: {INPUT_FILE}")
        return

    nlp = load_ner_model()
    workbook = pd.read_excel(INPUT_FILE, sheet_name=None)
    processed_sheets = {}

    for sheet_name, df in workbook.items():
        print(f"Processing sheet: {sheet_name}")
        processed_sheets[sheet_name] = process_sheet(df, nlp)

    with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
        for sheet_name, df in processed_sheets.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)

    print(f"Completed. Output saved to: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
