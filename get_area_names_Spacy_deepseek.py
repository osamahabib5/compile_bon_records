import json
import os
import re

import pandas as pd
from geopy.extra.rate_limiter import RateLimiter
from geopy.geocoders import Nominatim
from openai import OpenAI


# --- 1. SETTINGS ---
INPUT_FILE = "notes_sample.xlsx"
OUTPUT_FILE = "Extracted_Geographic_Validation_deepseek.xlsx"
NOTES_COLUMN = "Notes"

DEEPSEEK_MODEL = "deepseek-v4-flash"
DEEPSEEK_BASE_URL = "https://api.deepseek.com"
MAX_NOTE_CHARS_FOR_LLM = 12000

geolocator = Nominatim(user_agent="historical_origin_extractor_deepseek_v2", timeout=10)
geocode_service = RateLimiter(geolocator.geocode, min_delay_seconds=1.1)

GEO_CACHE = {}
LLM_CACHE = {}
DEEPSEEK_CLIENT = None


# --- 2. DEEPSEEK HELPERS ---
def get_deepseek_client():
    """Create the DeepSeek client lazily so import-time setup stays lightweight."""
    global DEEPSEEK_CLIENT

    if DEEPSEEK_CLIENT is None:
        api_key = os.environ.get("DEEPSEEK_API_KEY")
        if not api_key:
            raise RuntimeError("DEEPSEEK_API_KEY environment variable is not set.")

        DEEPSEEK_CLIENT = OpenAI(api_key=api_key, base_url=DEEPSEEK_BASE_URL)

    return DEEPSEEK_CLIENT


def strip_code_fences(text):
    """Remove optional Markdown fences so JSON parsing has a clean input."""
    cleaned = (text or "").strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"\s*```$", "", cleaned)
    return cleaned.strip()


def parse_json_payload(content, field_name):
    """Parse a simple JSON object and return a single named field."""
    cleaned = strip_code_fences(content)
    candidates = [cleaned]

    obj_start = cleaned.find("{")
    obj_end = cleaned.rfind("}")
    if obj_start != -1 and obj_end != -1 and obj_end > obj_start:
        candidates.append(cleaned[obj_start : obj_end + 1])

    for candidate in candidates:
        if not candidate:
            continue
        try:
            payload = json.loads(candidate)
        except json.JSONDecodeError:
            continue

        if isinstance(payload, dict):
            value = payload.get(field_name, "")
            return str(value).strip()

    return ""


def is_rate_limit_error(exc):
    """Best-effort detection for provider rate-limit responses."""
    message = str(exc).lower()
    return "rate limit" in message or "429" in message or "too many requests" in message


# Titles and non-geographic short forms to exclude
TITLE_WORDS = {"Mr", "Mrs", "Ms", "Dr", "Prof", "Esq", "KAD", "A.L", "mo", "Master", "Lt", "Capt", "Jnr", "Snr", "Sr", "Jr"}

# US States for validation
US_STATES = {
    "Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado", "Connecticut",
    "Delaware", "Florida", "Georgia", "Hawaii", "Idaho", "Illinois", "Indiana", "Iowa",
    "Kansas", "Kentucky", "Louisiana", "Maine", "Maryland", "Massachusetts", "Michigan",
    "Minnesota", "Mississippi", "Missouri", "Montana", "Nebraska", "Nevada", "New Hampshire",
    "New Jersey", "New Mexico", "New York", "North Carolina", "North Dakota", "Ohio",
    "Oklahoma", "Oregon", "Pennsylvania", "Rhode Island", "South Carolina", "South Dakota",
    "Tennessee", "Texas", "Utah", "Vermont", "Virginia", "Washington", "West Virginia",
    "Wisconsin", "Wyoming"
}

def apply_historical_area_fixes(area_text, note_text=""):
    """Normalize historically inconsistent place names before geocoding."""
    if not area_text:
        return ""

    cleaned = str(area_text).strip().strip(" ,;:.")
    if not cleaned or cleaned in {"-", "None", "null", "N/A"}:
        return ""

    cleaned = re.sub(r"\s+", " ", cleaned)
    lower_cleaned = cleaned.lower()
    lower_note = str(note_text).lower()

    if "wanda river" in lower_cleaned:
        cleaned = re.sub(r"(?i)wanda river", "Wando River", cleaned)
        lower_cleaned = cleaned.lower()

    if "charles town" in lower_cleaned:
        cleaned = re.sub(r"(?i)charles town", "Charleston", cleaned)
        lower_cleaned = cleaned.lower()

    if "charlestown" in lower_cleaned:
        if "south carolina" in lower_cleaned or "south carolina" in lower_note:
            cleaned = re.sub(r"(?i)charlestown", "Charleston", cleaned)
        elif cleaned.lower() == "charlestown":
            cleaned = "Charleston, South Carolina"

    cleaned = cleaned.strip(" ,;:.")
    return cleaned


def heuristic_origin_area(note_text):
    """
    Fallback extractor for cases where DeepSeek is unavailable.
    Prioritizes origin phrases and returns one connected place string.
    """
    text = re.sub(r"\s+", " ", str(note_text)).strip()
    if not text:
        return ""

    patterns = [
        r"formerly slave to .*?,\s*([^.;]+)",
        r"born at\s+([^.;]+)",
        r"born in\s+([^.;]+)",
        r"native of\s+([^.;]+)",
        r"from\s+([^.;]+)",
        r"near\s+([^.;]+)",
    ]

    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            area = match.group(1)
            area = re.split(r"\b(?:whom|who|which|left|sold|claimed by|claimant)\b", area, maxsplit=1, flags=re.IGNORECASE)[0]
            area = area.strip(" ,;:.()")
            return apply_historical_area_fixes(area, text)

    return ""


def call_deepseek_json(system_prompt, user_prompt, cache_key, field_name):
    """Call DeepSeek and parse a single JSON field."""
    if cache_key in LLM_CACHE:
        return LLM_CACHE[cache_key]

    client = get_deepseek_client()

    try:
        response = client.chat.completions.create(
            model=DEEPSEEK_MODEL,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            stream=False,
            temperature=0,
        )
        content = response.choices[0].message.content if response.choices else ""
        parsed = parse_json_payload(content, field_name)
        LLM_CACHE[cache_key] = parsed
        return parsed
    except Exception as exc:
        if not is_rate_limit_error(exc):
            print(f"DeepSeek request failed: {exc}")
        LLM_CACHE[cache_key] = ""
        return ""


def extract_origin_area_from_notes(note_text):
    """
    Use DeepSeek to extract one origin area from the Notes column.
    """
    note_text = "" if pd.isna(note_text) else str(note_text).strip()
    if not note_text or note_text == "-":
        return ""

    llm_note_text = note_text[:MAX_NOTE_CHARS_FOR_LLM]
    cache_key = f"origin::{llm_note_text}"

    system_prompt = (
        "You extract one origin place from a historical note for downstream geocoding. "
        "Return strict JSON only in the form {\"area\": \"...\"}. "
        "Choose the place most directly tied to the person's origin, birthplace, former location, "
        "or the first prior location clearly associated with the person. "
        "Do not choose the claimant's residence or a later destination unless that is the only place. "
        "Keep connected place names together, such as Hackensack, New Jersey. "
        "If the note contains New York Island, return New York Island. "
        "If the note says near Hackensack, New Jersey, return Hackensack, New Jersey. "
        "If a South Carolina place is written as Charlestown or Charles Town, return Charleston, South Carolina. "
        "If Wanda River appears, return Wando River. "
        "If no valid origin place exists, return {\"area\": \"\"}."
    )

    user_prompt = (
        "Identify the single best origin place for the person in this note.\n"
        "Return JSON only.\n\n"
        f"Note:\n{llm_note_text}"
    )

    extracted = call_deepseek_json(system_prompt, user_prompt, cache_key, "area")
    extracted = apply_historical_area_fixes(extracted, note_text)

    if extracted:
        return extracted

    return heuristic_origin_area(note_text)


def is_us_state(component):
    """
    Check if a component is a US state name.
    """
    component = str(component).strip()
    return component in US_STATES


def is_geographic_location(area_component):
    """
    Validate if a component is a real geographic location using geopy.
    Returns True if geocodable, False otherwise.
    """
    component = str(area_component).strip()
    if not component or len(component) < 2:
        return False
    
    # Filter out titles and non-geographic terms
    if component in TITLE_WORDS or component.upper() in TITLE_WORDS:
        return False
    
    # Try to geocode it
    try:
        location = geocode_service(component, addressdetails=True, timeout=5)
        return location is not None
    except Exception:
        return False


def validate_and_reconstruct_area(raw_area, note_text=""):
    """
    Intelligently validate and reconstruct area names by checking combinations.
    
    Strategy:
    1. First check if the entire area is valid
    2. If the last component is a US state, work backwards from the end:
       - Try increasingly longer combinations ending with the state
    3. If last component is NOT a US state, work forwards from the beginning:
       - Try increasingly longer combinations starting from the first component
    4. Return the first valid combination found, or empty string if none found
    """
    if not raw_area:
        return ""
    
    raw_area = apply_historical_area_fixes(raw_area, note_text)
    if not raw_area:
        return ""
    
    # Try the entire area first
    if is_geographic_location(raw_area):
        return raw_area
    
    # Split by comma to get individual components
    components = [c.strip() for c in raw_area.split(",")]
    
    # Filter out titles
    components = [c for c in components if c and c not in TITLE_WORDS and c.upper() not in TITLE_WORDS]
    
    if not components:
        return ""
    
    # Check if the last component is a US state
    last_component = components[-1]
    is_last_us_state = is_us_state(last_component)
    
    if is_last_us_state:
        # Work backwards from the end: try longer and longer combinations
        # Start with just the state, then add the next component, etc.
        for i in range(len(components), 0, -1):
            combo = ", ".join(components[i-1:])
            if is_geographic_location(combo):
                return combo
    else:
        # Work forwards from the beginning: try longer and longer combinations
        # Start with first component, then add next, etc.
        for i in range(1, len(components) + 1):
            combo = ", ".join(components[:i])
            if is_geographic_location(combo):
                return combo
    
    return ""


# --- 3. GEOCODING HELPERS ---
def clean_single_value(val):
    """Convert a geocoder field into a trimmed single display value."""
    if val is None or pd.isna(val):
        return ""

    cleaned = str(val).strip()
    if not cleaned:
        return ""

    return cleaned.split(",")[0].strip()


def get_geopy_data_cached(query, priority_us=False):
    """
    Geocode with caching.

    When priority_us=True, try a US-restricted lookup first, then fall back to a global lookup.
    """
    cache_key = f"{query}_US" if priority_us else f"{query}_GLOBAL"
    if cache_key in GEO_CACHE:
        return GEO_CACHE[cache_key]

    try:
        if priority_us:
            location = geocode_service(query, addressdetails=True, country_codes="us")
            if location:
                GEO_CACHE[cache_key] = location
                return location

        location = geocode_service(query, addressdetails=True)
        GEO_CACHE[cache_key] = location
        return location
    except Exception:
        GEO_CACHE[cache_key] = None
        return None


def should_prioritize_us(area_text, note_text=""):
    """Prefer US lookups when the text already signals a US context."""
    combined_text = f"{area_text} {note_text}".lower()
    us_markers = [
        "south carolina",
        "north carolina",
        "virginia",
        "georgia",
        "maryland",
        "new york",
        "new jersey",
        "pennsylvania",
        "connecticut",
        "massachusetts",
        "united states",
        "usa",
    ]
    return any(marker in combined_text for marker in us_markers)


def classify_area(area_text, note_text=""):
    """
    Geocode one extracted area and populate the administrative columns.
    """
    cols = ["Validation", "City", "Landmark", "County", "State", "Country"]
    result = {col: "" for col in cols}
    if not area_text:
        return result

    loc = get_geopy_data_cached(area_text, priority_us=should_prioritize_us(area_text, note_text))
    if not loc:
        result["Validation"] = "No"
        return result

    result["Validation"] = "Yes"
    addr = loc.raw.get("address", {})
    address_type = loc.raw.get("addresstype", "").lower()
    primary_label = clean_single_value(area_text)

    city_value = clean_single_value(addr.get("city") or addr.get("town") or addr.get("village"))
    county_value = clean_single_value(addr.get("county"))
    state_value = clean_single_value(addr.get("state") or addr.get("province") or addr.get("state_district"))
    country_value = clean_single_value(addr.get("country"))

    if address_type in {"city", "town", "village", "hamlet", "municipality", "suburb"}:
        result["City"] = primary_label or city_value
    elif address_type in {"county", "district", "county_district"}:
        result["County"] = primary_label or county_value
    elif address_type in {"state", "province", "state_district"}:
        result["State"] = primary_label or state_value
    elif address_type == "country":
        result["Country"] = primary_label or country_value
    else:
        result["Landmark"] = primary_label

    if not result["City"]:
        result["City"] = city_value
    if not result["County"]:
        result["County"] = county_value
    if not result["State"]:
        result["State"] = state_value
    if not result["Country"]:
        result["Country"] = country_value

    return result


def resolve_single_coordinate(area_text, note_text=""):
    """
    Produce one coordinate set per area using geopy only.
    No DeepSeek involvement in coordinate resolution.
    """
    if not area_text:
        return "", ""

    # Use geopy directly to geocode the area
    loc = get_geopy_data_cached(area_text, priority_us=should_prioritize_us(area_text, note_text))

    if not loc:
        return area_text, ""

    return area_text, f"{loc.latitude}, {loc.longitude}"


# --- 4. DATAFRAME PROCESSING ---
def process_record(note_text):
    """
    1. Extract one origin area from Notes with DeepSeek.
    2. Validate and reconstruct the area with geopy (remove invalid/title components).
    3. Keep connected place names together.
    4. Populate City/Landmark/County/State/Country from the validated area.
    5. Leave coordinate fields blank for now; they are filled in a unique-area pass later.
    """
    cols = [
        "Areas",
        "Validation",
        "City",
        "Landmark",
        "County",
        "State",
        "Country",
        "Areas_for_coordinates",
        "Final_Coordinates",
    ]
    result = {col: "" for col in cols}

    if pd.isna(note_text) or str(note_text).strip() in {"", "-"}:
        return pd.Series([result[col] for col in cols], index=cols)

    # Extract raw area from notes
    area = extract_origin_area_from_notes(note_text)
    area = apply_historical_area_fixes(area, note_text)

    if not area:
        return pd.Series([result[col] for col in cols], index=cols)
    
    # Validate and reconstruct area, removing invalid components and titles
    area = validate_and_reconstruct_area(area, note_text)
    
    if not area:
        return pd.Series([result[col] for col in cols], index=cols)

    result["Areas"] = area
    classified = classify_area(area, note_text)
    result.update(classified)

    return pd.Series([result[col] for col in cols], index=cols)


def populate_coordinates_from_unique_areas(df):
    """
    Resolve one coordinate set per unique area, then map it back to all rows.
    """
    unique_areas = [area for area in df["Areas"].dropna().astype(str).str.strip().unique() if area]
    area_to_query = {}
    area_to_coords = {}

    print(f"Resolving coordinates for {len(unique_areas)} unique area values...")

    for area in unique_areas:
        query, coords = resolve_single_coordinate(area, area)
        area_to_query[area] = query
        area_to_coords[area] = coords

    df["Areas_for_coordinates"] = df["Areas"].map(area_to_query).fillna("")
    df["Final_Coordinates"] = df["Areas"].map(area_to_coords).fillna("")
    return df


def replace_empty_with_dash(df):
    """
    Replace all empty strings and NaN values with '-' in all columns.
    """
    df = df.fillna("-")  # Replace NaN values
    df = df.replace("", "-")  # Replace empty strings
    return df


# --- 5. MAIN EXECUTION ---
def main():
    print(f"Reading {INPUT_FILE}...")
    try:
        df = pd.read_excel(INPUT_FILE)
    except Exception as exc:
        print(f"Error reading input file: {exc}")
        return

    if NOTES_COLUMN not in df.columns:
        print(f"Error: column '{NOTES_COLUMN}' not found in {INPUT_FILE}.")
        return

    print(f"Extracting one origin area per note with {DEEPSEEK_MODEL}...")
    processed_df = df[NOTES_COLUMN].apply(process_record)
    final_df = pd.concat([df, processed_df], axis=1)

    final_df = populate_coordinates_from_unique_areas(final_df)
    
    print("Replacing empty values with '-'...")
    final_df = replace_empty_with_dash(final_df)

    print(f"Saving results to {OUTPUT_FILE}...")
    final_df.to_excel(OUTPUT_FILE, index=False)
    print("Done.")


if __name__ == "__main__":
    main()
