import pandas as pd
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

# --- SETTINGS ---
INPUT_FILE = "Extracted_Geographic_Validation_deepseek_v1.xlsx"
OUTPUT_FILE = "Extracted_Geographic_Validation_deepseek_v2.xlsx"
AMENDS_COLUMN = "Amends_Needed"
AREA_COLUMN = "Areas"

geolocator = Nominatim(user_agent="amendments_geocoder_v1", timeout=10)
geocode_service = RateLimiter(geolocator.geocode, min_delay_seconds=1.1)

GEO_CACHE = {}


def get_geopy_data_cached(query, priority_us=False):
    """
    Geocode with caching.
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


def should_prioritize_us(area_text):
    """Prefer US lookups when the text already signals a US context."""
    area_lower = str(area_text).lower()
    us_markers = [
        "south carolina", "north carolina", "virginia", "georgia", "maryland",
        "new york", "new jersey", "pennsylvania", "connecticut", "massachusetts",
        "united states", "usa",
    ]
    return any(marker in area_lower for marker in us_markers)


def clean_single_value(val):
    """Convert a geocoder field into a trimmed single display value."""
    if val is None:
        return ""
    
    cleaned = str(val).strip()
    if not cleaned:
        return ""
    
    return cleaned.split(",")[0].strip()


def get_area_data_for_amendment(area_text):
    """
    Get all geographic data for an area using geopy.
    Returns a dict with City, Landmark, County, State, Country, Areas_for_coordinates, and Final_Coordinates.
    """
    if not area_text or area_text == "-":
        return {
            "City": "",
            "Landmark": "",
            "County": "",
            "State": "",
            "Country": "",
            "Areas_for_coordinates": "",
            "Final_Coordinates": ""
        }

    loc = get_geopy_data_cached(area_text, priority_us=should_prioritize_us(area_text))
    
    result = {
        "City": "",
        "Landmark": "",
        "County": "",
        "State": "",
        "Country": "",
        "Areas_for_coordinates": area_text,
        "Final_Coordinates": ""
    }
    
    if not loc:
        return result

    result["Final_Coordinates"] = f"{loc.latitude}, {loc.longitude}"
    
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


def main():
    print(f"Reading {INPUT_FILE}...")
    try:
        df = pd.read_excel(INPUT_FILE)
    except Exception as exc:
        print(f"Error reading input file: {exc}")
        return

    if AMENDS_COLUMN not in df.columns:
        print(f"Error: column '{AMENDS_COLUMN}' not found in {INPUT_FILE}.")
        return

    if AREA_COLUMN not in df.columns:
        print(f"Error: column '{AREA_COLUMN}' not found in {INPUT_FILE}.")
        return

    # Filter rows where Amends_Needed == "Yes"
    amend_df = df[df[AMENDS_COLUMN].astype(str).str.strip() == "Yes"].copy()
    
    if amend_df.empty:
        print("No rows found with Amends_Needed = 'Yes'. No updates needed.")
        return

    print(f"Found {len(amend_df)} rows with Amends_Needed = 'Yes'")

    # Get unique areas that need amendment
    unique_areas = [area for area in amend_df[AREA_COLUMN].dropna().astype(str).str.strip().unique() if area and area != "-"]
    
    if not unique_areas:
        print("No valid areas found in amendment records.")
        return

    print(f"Processing {len(unique_areas)} unique areas for amendment resolution...")

    # Get all geographic data for each unique area
    area_to_data = {}
    for i, area in enumerate(unique_areas, 1):
        data = get_area_data_for_amendment(area)
        area_to_data[area] = data
        coords = data.get("Final_Coordinates", "")
        if coords:
            print(f"  [{i}/{len(unique_areas)}] {area} → {coords}")
        else:
            print(f"  [{i}/{len(unique_areas)}] {area} → No coordinates found")

    # Update all columns for rows with amendments
    amend_mask = df[AMENDS_COLUMN].astype(str).str.strip() == "Yes"
    
    # For each column, extract and update
    for col in ["City", "Landmark", "County", "State", "Country", "Areas_for_coordinates", "Final_Coordinates"]:
        df.loc[amend_mask, col] = (
            df.loc[amend_mask, AREA_COLUMN]
            .apply(lambda area: area_to_data.get(area, {}).get(col, ""))
        )
    
    # Update Validation column based on whether coordinates were found
    df.loc[amend_mask, "Validation"] = (
        df.loc[amend_mask, "Final_Coordinates"]
        .apply(lambda coord: "Yes" if coord and coord != "" else "No")
    )

    # Replace empty values with '-'
    df = df.fillna("-")
    df = df.replace("", "-")

    print(f"Saving updated results to {OUTPUT_FILE}...")
    df.to_excel(OUTPUT_FILE, index=False)
    print("Done. Amendment coordinates have been updated.")


if __name__ == "__main__":
    main()
