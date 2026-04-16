import pandas as pd
import re
import time
import os
import json
from datetime import datetime, timedelta
from tqdm import tqdm
from dotenv import load_dotenv
from groq import Groq

try:
    import spacy
except ImportError:
    spacy = None

try:
    from geopy.geocoders import Nominatim
except ImportError:
    Nominatim = None

# --- 1. CONFIGURATION ---
load_dotenv()
client = Groq(api_key=os.getenv("GROQ_API_KEY"))

MODEL_NAME = "openai/gpt-oss-120b"
EXPECTED_OUTPUT_COLUMNS = 33
BIRTH_BASE_YEAR = 1783
SHIP_CONTEXT_FIELDS = ["Ship_Name", "Commander", "Arrival_Port", "Arrival_Port_Country"]
LLM_TARGET_COLUMNS = [
    "Extracted_City",
    "Extracted_County",
    "Extracted_State",
    "Extracted_Area",
    "Country",
    "Origination_Port",
    "Departure_Port",
    "Arrival_Port",
    "Arrival_Port_Country",
    "Commander",
    "Ship_Name"
]

# OpenAI GPT OSS 120B production limits: 8K TPM / 30 RPM / 200K daily
TPM_LIMIT = 8000
RPM_LIMIT = 30
DAILY_TOKEN_LIMIT = 200000
DAILY_REQUEST_LIMIT = 1000
REQUEST_DELAY = 2.0
GEOPY_REQUEST_DELAY = 1.1

INPUT_FILE = "Consolidated_Directory_v12_subset.xlsx"
OUTPUT_FILE = "Validated_Records_Cleaned.xlsx"
USAGE_LOG_FILE = "groq_openai_oss_usage_log.json"
COUNTY_LOOKUP_FILE = "US_Counties_Coordinates.xlsx"

INPUT_COST_1M = 0.29
OUTPUT_COST_1M = 0.59

VALIDATED_COLUMNS = [
    "ID", "Book", "First_Name", "Surname", "Ship_Name", "Notes", "Ship_Notes",
    "Birthdate", "Gender", "Race", "Ethnicity", "Origin", "Extracted_City",
    "Extracted_County", "Extracted_State", "Extracted_Area", "Country",
    "Departure_Coordinates", "Origination_Port", "Departure_Port",
    "Departure_Date", "Arrival_Port", "Arrival_Port_Country", "Arrival_Coordinates",
    "Father_FirstName", "Father_Surname", "Mother_FirstName", "Mother_Surname",
    "Ref_Page", "Commander", "Enslaver", "Primary_Source_1", "Primary_Source_2"
]
ALLOWED_GENDERS = {"Male", "Female", "Child Male", "Child Female", "Unknown", "-"}
ALLOWED_RACES = {"Black", "White", "Mulatto", "Quadroon", "Mestizo", "Indigenous", "Asian", "Unknown", "-"}
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
KNOWN_COUNTRIES = {"United States", "Canada", "United Kingdom", "Jamaica", "Bahamas", "Barbados", "Africa"}
SHIP_NOTES_COUNTRIES = KNOWN_COUNTRIES | {"Germany", "England", "Britain", "France"}
ARRIVAL_PORT_COUNTRIES = {
    "St. John's": "Canada",
    "St Johns": "Canada",
    "Port Roseway": "Canada",
    "Quebec": "Canada",
    "Halifax": "Canada",
    "Shelburne": "Canada",
    "Annapolis Royal": "Canada",
    "Birchtown": "Canada",
    "Kingston": "Jamaica",
    "London": "United Kingdom"
}
TITLE_PREFIXES = ("mr ", "mrs ", "ms ", "miss ", "dr ", "capt ", "captain ", "master ")
COUNTY_HINTS = (" county", " parish")
SYSTEM_PROMPT = f"""Historical data validator for Book of Negroes records (1783).

Return exactly {EXPECTED_OUTPUT_COLUMNS} pipe-separated values.
Only modify or fill: Extracted_City, Extracted_County, Extracted_State, Extracted_Area, Country, Origination_Port, Departure_Port, Arrival_Port, Arrival_Port_Country, Commander, Ship_Name.
Do not change Primary_Source_1 or Primary_Source_2.
Keep existing non-empty values unless they are clearly wrong for that field and derived from the allowed source.
Only use Notes for: Extracted_City, Extracted_County, Extracted_State, Extracted_Area, Country, Origination_Port, Departure_Port.
Only use Ship_Notes for: Arrival_Port, Arrival_Port_Country, Ship_Name, Commander.
Do not use Ship_Notes to fill any extracted city, county, state, area, country, origination port, or departure port values.
Do not use Notes to fill Arrival_Port, Arrival_Port_Country, Ship_Name, or Commander.
Do not generate or edit Departure_Coordinates or Arrival_Coordinates.
Rules:
- Extracted_City = city only
- Extracted_County = county or region only
- Extracted_State = valid US state only
- Extracted_Area = area only
- Arrival_Port = specific port only, not a state or country
- Country and Arrival_Port_Country = sovereign country only
- Commander = valid human person name only
- Treat St. John's and Port Roseway as arrival ports, not commanders
- If a field is wrong-type, move it to the right field when possible
- If Commander is blank or invalid, set '-'
- If a required field cannot be determined from the allowed source, return '-'
No guessing. No headers. Values only."""


def load_spacy_pipeline():
    if spacy is None:
        return None
    for model_name in ("en_core_web_md", "en_core_web_sm"):
        try:
            return spacy.load(model_name)
        except OSError:
            continue
    return None


NLP = load_spacy_pipeline()
geolocator = Nominatim(user_agent="bon_openai_oss_cleaner") if Nominatim is not None else None


# --- 2. USAGE & RATE LIMIT TRACKER ---

class UsageTracker:
    def __init__(self, log_file):
        self.log_file = log_file
        self.session_in_tokens = 0
        self.session_out_tokens = 0
        if not os.path.exists(self.log_file):
            with open(self.log_file, "w", encoding="utf-8") as f:
                json.dump([], f)

    def log_request(self, in_tokens, out_tokens):
        self.session_in_tokens += in_tokens
        self.session_out_tokens += out_tokens
        entry = {
            "timestamp": datetime.now().isoformat(),
            "input_tokens": in_tokens,
            "output_tokens": out_tokens,
            "total_tokens": in_tokens + out_tokens,
        }
        try:
            with open(self.log_file, "r+", encoding="utf-8") as f:
                data = json.load(f)
                data.append(entry)
                f.seek(0)
                json.dump(data, f)
                f.truncate()
        except Exception:
            pass

    def get_window_stats(self, seconds=60):
        cutoff = datetime.now() - timedelta(seconds=seconds)
        try:
            with open(self.log_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                window_data = [e for e in data if datetime.fromisoformat(e["timestamp"]) > cutoff]
                return len(window_data), sum(e["total_tokens"] for e in window_data)
        except Exception:
            return 0, 0

    def get_daily_usage(self):
        cutoff = datetime.now() - timedelta(hours=24)
        try:
            with open(self.log_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                daily_data = [e for e in data if datetime.fromisoformat(e["timestamp"]) > cutoff]
                return sum(e["total_tokens"] for e in daily_data)
        except Exception:
            return 0

    def get_daily_request_count(self):
        cutoff = datetime.now() - timedelta(hours=24)
        try:
            with open(self.log_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                daily_data = [e for e in data if datetime.fromisoformat(e["timestamp"]) > cutoff]
                return len(daily_data)
        except Exception:
            return 0

    def print_session_summary(self):
        cost = ((self.session_in_tokens / 1_000_000) * INPUT_COST_1M) + (
            (self.session_out_tokens / 1_000_000) * OUTPUT_COST_1M
        )
        print("\n" + "=" * 45)
        print("OpenAI OSS 120B Cleaning Summary")
        print(f"Total Tokens:   {self.session_in_tokens + self.session_out_tokens:,}")
        print(f" - Input:       {self.session_in_tokens:,}")
        print(f" - Output:      {self.session_out_tokens:,}")
        print(f"Total Cost:     ${cost:.4f}")
        print("=" * 45)

    def print_limit_status(self, prefix="Limit Status"):
        rpm_used, tpm_used = self.get_window_stats(60)
        daily_used = self.get_daily_usage()
        daily_requests_used = self.get_daily_request_count()
        rpm_remaining = max(RPM_LIMIT - rpm_used, 0)
        tpm_remaining = max(TPM_LIMIT - tpm_used, 0)
        daily_remaining = max(DAILY_TOKEN_LIMIT - daily_used, 0)
        status = (
            f"{prefix} | "
            f"RPM remaining: {rpm_remaining}/{RPM_LIMIT} | "
            f"TPM remaining: {tpm_remaining}/{TPM_LIMIT} | "
            f"Daily tokens remaining: {daily_remaining:,}/{DAILY_TOKEN_LIMIT:,}"
        )
        if DAILY_REQUEST_LIMIT is not None:
            daily_requests_remaining = max(DAILY_REQUEST_LIMIT - daily_requests_used, 0)
            status += f" | Daily requests remaining: {daily_requests_remaining:,}/{DAILY_REQUEST_LIMIT:,}"
        print(status)


tracker = UsageTracker(USAGE_LOG_FILE)


# --- 3. PROMPT GENERATOR ---

def normalize_value(value):
    if pd.isna(value):
        return "-"
    text = str(value).strip()
    return text if text and text.lower() != "nan" else "-"


def get_validation_prompt(current_values_dict):
    normalized_values = {
        col: normalize_value(current_values_dict.get(col, "-"))
        for col in VALIDATED_COLUMNS
    }
    current_values_str = build_compact_record_context(normalized_values)

    return f"""CURRENT RECORD
{current_values_str}

OUTPUT ORDER
ID | Book | First_Name | Surname | Ship_Name | Notes | Ship_Notes | Birthdate | Gender | Race | Ethnicity | Origin | Extracted_City | Extracted_County | Extracted_State | Extracted_Area | Country | Departure_Coordinates | Origination_Port | Departure_Port | Departure_Date | Arrival_Port | Arrival_Port_Country | Arrival_Coordinates | Father_FirstName | Father_Surname | Mother_FirstName | Mother_Surname | Ref_Page | Commander | Enslaver | Primary_Source_1 | Primary_Source_2

RESPONSE FORMAT
Return exactly {EXPECTED_OUTPUT_COLUMNS} pipe-separated values only.
"""


def build_compact_record_context(normalized_values):
    preserved_fields = [
        "ID", "Book", "Ship_Name", "Ref_Page", "Primary_Source_1", "Primary_Source_2"
    ]
    mutable_priority_fields = list(LLM_TARGET_COLUMNS)

    preserved_lines = [f"{field}: {normalized_values[field]}" for field in preserved_fields]
    mutable_lines = [
        f"{field}: {normalized_values[field]}"
        for field in mutable_priority_fields
        if normalized_values[field] != "-"
    ]

    if not mutable_lines:
        mutable_lines = ["All target fields currently '-'."]

    notes_needed = any(
        normalized_values[field] == "-"
        for field in ["Extracted_City", "Extracted_County", "Extracted_State", "Extracted_Area"]
    )

    return "\n".join([
        "SOURCE_TEXT:",
        f"Notes: {normalized_values['Notes'] if notes_needed else '[not needed if location fields already populated]'}",
        f"Ship_Notes: {normalized_values['Ship_Notes']}",
        "",
        "PRESERVE_OR_MATCH:",
        *preserved_lines,
        "",
        "CURRENT_NONEMPTY_MUTABLE_FIELDS:",
        *mutable_lines,
    ])


def get_ship_context_key(ship_notes):
    ship_notes = normalize_value(ship_notes)
    return ship_notes.lower() if ship_notes != "-" else None


def apply_cached_ship_context(row_dict, ship_context_cache):
    cache_key = get_ship_context_key(row_dict.get("Ship_Notes", "-"))
    if not cache_key or cache_key not in ship_context_cache:
        return row_dict
    cached_context = ship_context_cache[cache_key]
    for field in SHIP_CONTEXT_FIELDS:
        if normalize_value(row_dict.get(field, "-")) == "-" and normalize_value(cached_context.get(field, "-")) != "-":
            row_dict[field] = cached_context[field]
    return row_dict


def update_ship_context_cache(ship_context_cache, row_dict):
    cache_key = get_ship_context_key(row_dict.get("Ship_Notes", "-"))
    if not cache_key:
        return
    ship_context_cache[cache_key] = {
        field: normalize_value(row_dict.get(field, "-"))
        for field in SHIP_CONTEXT_FIELDS
    }


def extract_entities(text, labels):
    if NLP is None or normalize_value(text) == "-":
        return []
    doc = NLP(text)
    return [ent.text.strip() for ent in doc.ents if ent.label_ in labels]


def parse_ship_notes_locally(row_dict):
    ship_notes = normalize_value(row_dict.get("Ship_Notes", "-"))
    if ship_notes == "-":
        return row_dict

    arrival_port = normalize_value(row_dict.get("Arrival_Port", "-"))
    arrival_country = normalize_value(row_dict.get("Arrival_Port_Country", "-"))
    commander = normalize_value(row_dict.get("Commander", "-"))

    destination_match = re.search(r"bound for\s+(.+)$", ship_notes, flags=re.IGNORECASE)
    if destination_match:
        dest_segment = destination_match.group(1).strip(" .,:;")
        found_locs = extract_entities(dest_segment, {"GPE", "LOC"})
        found_people = extract_entities(dest_segment, {"PERSON"})

        candidate_loc = found_locs[0] if found_locs else infer_arrival_port(ship_notes)
        candidate_loc = normalize_output_value(re.sub(r".*?&\s*", "", candidate_loc).strip()) if candidate_loc != "-" else "-"

        if candidate_loc in SHIP_NOTES_COUNTRIES:
            if arrival_country == "-":
                arrival_country = candidate_loc
        elif candidate_loc != "-" and arrival_port == "-":
            arrival_port = candidate_loc
            if arrival_country == "-":
                arrival_country = ARRIVAL_PORT_COUNTRIES.get(candidate_loc, "Canada")

        if found_people and commander == "-":
            commander = compact_name(found_people[0])
        elif commander == "-":
            commander = infer_commander(ship_notes)

    if arrival_port != "-":
        row_dict["Arrival_Port"] = arrival_port
    if arrival_country != "-":
        row_dict["Arrival_Port_Country"] = arrival_country
    if commander != "-":
        row_dict["Commander"] = commander
    return row_dict


def extract_notes_locally(row_dict):
    notes = normalize_value(row_dict.get("Notes", "-"))
    if notes == "-":
        return row_dict

    needs_location_extraction = any(
        normalize_value(row_dict.get(field, "-")) == "-"
        for field in ["Extracted_City", "Extracted_County", "Extracted_State", "Extracted_Area"]
    )

    if normalize_value(row_dict.get("Enslaver", "-")) == "-":
        enslaver_match = re.search(r"\((.*?)\)", notes)
        if enslaver_match:
            content = enslaver_match.group(1).strip()
            if "own bottom" not in content.lower():
                row_dict["Enslaver"] = content

    if needs_location_extraction:
        clean_notes = re.sub(r"\(.*?\)", "", notes).strip()
        gpes = extract_entities(clean_notes, {"GPE", "LOC"})

        if gpes:
            if normalize_value(row_dict.get("Extracted_City", "-")) == "-":
                row_dict["Extracted_City"] = gpes[0]
            if len(gpes) >= 2 and normalize_value(row_dict.get("Extracted_State", "-")) == "-":
                row_dict["Extracted_State"] = gpes[1]

        if normalize_value(row_dict.get("Country", "-")) == "-":
            row_dict["Country"] = "United States"

    return row_dict


def apply_local_rule_engine(row_dict):
    row_dict = dict(row_dict)
    row_dict = parse_ship_notes_locally(row_dict)
    row_dict = extract_notes_locally(row_dict)
    row_dict = move_misplaced_values({
        col: normalize_value(row_dict.get(col, "-"))
        for col in VALIDATED_COLUMNS
    })
    row_dict = apply_geography_repairs(row_dict)
    row_dict = apply_voyage_repairs(row_dict)
    return row_dict


# --- 4. PARSING & POST-PROCESSING ---

def normalize_output_value(value):
    value = normalize_value(value)
    if value == "-":
        return value
    return re.sub(r"\s+", " ", value)


def parse_output(raw):
    lines = [l.strip() for l in raw.splitlines() if "|" in l]
    if not lines:
        return ["-"] * EXPECTED_OUTPUT_COLUMNS
    data_line = max(lines, key=lambda l: l.count("|"))
    parts = [p.strip() if p.strip() else "-" for p in data_line.split("|")]
    if "First_Name" in parts or "Ship_Notes" in parts:
        return ["-"] * EXPECTED_OUTPUT_COLUMNS
    padded = (parts + ["-"] * EXPECTED_OUTPUT_COLUMNS)[:EXPECTED_OUTPUT_COLUMNS]
    return [normalize_output_value(part) for part in padded]


def is_numeric_string(value):
    return bool(re.fullmatch(r"\d+", normalize_value(value)))


def is_year_string(value):
    return bool(re.fullmatch(r"\d{4}", normalize_value(value)))


def is_coordinate_string(value):
    return bool(re.fullmatch(r"-?\d+(?:\.\d+)?\s*,\s*-?\d+(?:\.\d+)?", normalize_value(value)))


def compact_name(value):
    value = normalize_output_value(value)
    if value == "-":
        return value
    cleaned = re.sub(r"\b(Jr|Sr)\.?\b", "", value, flags=re.IGNORECASE).strip(", ").strip()
    return cleaned or "-"


def clean_first_name(value):
    value = compact_name(value)
    if value == "-":
        return value
    pieces = value.split()
    first = pieces[0].strip(",. ")
    lowered = first.lower()
    for prefix in TITLE_PREFIXES:
        if lowered == prefix.strip():
            return pieces[1].strip(",. ") if len(pieces) > 1 else "-"
    return first or "-"


def clean_surname(value):
    value = compact_name(value)
    if value == "-":
        return value
    pieces = value.split()
    if not pieces:
        return "-"
    first_lower = pieces[0].lower()
    if any(first_lower == prefix.strip() for prefix in TITLE_PREFIXES):
        pieces = pieces[1:]
    return pieces[-1].strip(",. ") if pieces else "-"


def is_valid_commander_name(value):
    value = normalize_output_value(value)
    if value == "-":
        return False
    lower = value.lower()
    if lower in {key.lower() for key in ARRIVAL_PORT_COUNTRIES}:
        return False
    if lower in {state.lower() for state in US_STATES | KNOWN_COUNTRIES}:
        return False
    if re.search(r"\bport\b", lower):
        return False
    return bool(re.fullmatch(r"[A-Za-z'.-]+(?:\s+[A-Za-z'.-]+)*", value))


def validate_commander(record):
    commander = normalize_output_value(record.get("Commander", "-"))
    if not is_valid_commander_name(commander):
        record["Commander"] = "-"
    return record


def infer_birth_year(notes, current_birthdate):
    current_birthdate = normalize_value(current_birthdate)
    if is_year_string(current_birthdate):
        return current_birthdate
    match = re.search(r"\b(\d{1,2})\b", normalize_value(notes))
    if match:
        age = int(match.group(1))
        if 0 < age < 110:
            return str(BIRTH_BASE_YEAR - age)
    return "-"


def normalize_gender(value, notes):
    value = normalize_output_value(value)
    lowered = value.lower()
    if lowered in {"male", "man", "boy"}:
        return "Male"
    if lowered in {"female", "woman", "girl", "wench"}:
        return "Female"
    if value in ALLOWED_GENDERS:
        return value
    notes_lower = normalize_value(notes).lower()
    if "boy" in notes_lower:
        return "Child Male"
    if "girl" in notes_lower:
        return "Child Female"
    if "man" in notes_lower:
        return "Male"
    if "woman" in notes_lower or "wench" in notes_lower:
        return "Female"
    return "Unknown" if value != "-" else "-"


def normalize_race(value):
    value = normalize_output_value(value)
    lowered = value.lower()
    mapping = {
        "negro": "Black",
        "black": "Black",
        "white": "White",
        "mulatto": "Mulatto",
        "quadroon": "Quadroon",
        "mestizo": "Mestizo",
        "indian": "Indigenous",
        "indigenous": "Indigenous",
        "asian": "Asian",
        "unknown": "Unknown",
    }
    if lowered in mapping:
        return mapping[lowered]
    return value if value in ALLOWED_RACES else "-"


def normalize_ethnicity(race, ethnicity):
    ethnicity = normalize_output_value(ethnicity)
    if race in {"Mulatto", "Quadroon"}:
        return "Mixed Race"
    if race == "Black":
        return ethnicity if ethnicity in {"African American", "-"} else "African American"
    return ethnicity


def infer_commander(ship_notes):
    ship_notes = normalize_value(ship_notes)
    if ship_notes == "-":
        return "-"

    patterns = [
        r"(?:captain|capt\.?|master|commander)\s+([A-Z][A-Za-z'.-]+(?:\s+[A-Z][A-Za-z'.-]+)+)",
        r"commanded by\s+([A-Z][A-Za-z'.-]+(?:\s+[A-Z][A-Za-z'.-]+)+)",
    ]
    for pattern in patterns:
        match = re.search(pattern, ship_notes, flags=re.IGNORECASE)
        if match:
            return compact_name(match.group(1))

    bound_match = re.search(r"bound for\s+(.+)$", ship_notes, flags=re.IGNORECASE)
    if bound_match:
        tail = bound_match.group(1).strip(" .,:;")

        if NLP is not None:
            people = extract_entities(tail, {"PERSON"})
            if people:
                return compact_name(people[-1])

        trailing_name = re.search(
            r"(?:[A-Z][A-Za-z'.-]+(?:\s+[A-Z][A-Za-z'.-]+)+)$",
            tail
        )
        if trailing_name:
            candidate = compact_name(trailing_name.group(0))
            arrival_port = infer_arrival_port(ship_notes)
            if candidate != normalize_value(arrival_port):
                return candidate

    return "-"


def infer_arrival_port(ship_notes):
    ship_notes = normalize_value(ship_notes)
    patterns = [
        r"bound for\s+([A-Za-z'. -]+)",
        r"for\s+([A-Za-z'. -]+)$",
        r"to\s+([A-Za-z'. -]+)$",
    ]
    for pattern in patterns:
        match = re.search(pattern, ship_notes, flags=re.IGNORECASE)
        if match:
            port = normalize_output_value(match.group(1).strip(" .,;:"))
            return port if not any(ch.isdigit() for ch in port) else "-"
    return "-"


def infer_country_from_state_or_port(record):
    if record["Extracted_State"] in US_STATES and record["Country"] == "-":
        record["Country"] = "United States"
    if record["Arrival_Port"] in ARRIVAL_PORT_COUNTRIES:
        record["Arrival_Port_Country"] = ARRIVAL_PORT_COUNTRIES[record["Arrival_Port"]]
    if record["Extracted_City"] == "Kingston" and record["Country"] == "-":
        record["Country"] = "Jamaica"
    if record["Extracted_City"] == "London" and record["Country"] == "-":
        record["Country"] = "United Kingdom"
    return record


def apply_geography_repairs(record):
    city = record["Extracted_City"]
    county = record["Extracted_County"]
    state = record["Extracted_State"]

    if city in US_STATES:
        record["Extracted_State"] = city
        record["Extracted_City"] = "-"
        city = "-"

    if city != "-" and city.lower().endswith(COUNTY_HINTS):
        record["Extracted_County"] = city
        record["Extracted_City"] = "-"
        county = record["Extracted_County"]

    if county in US_STATES:
        record["Extracted_State"] = county
        record["Extracted_County"] = "-"
        county = "-"

    if state in KNOWN_COUNTRIES:
        record["Country"] = state
        record["Extracted_State"] = "-"

    if record["Departure_Coordinates"] != "-" and not is_coordinate_string(record["Departure_Coordinates"]):
        record["Departure_Coordinates"] = "-"
    if record["Arrival_Coordinates"] != "-" and not is_coordinate_string(record["Arrival_Coordinates"]):
        record["Arrival_Coordinates"] = "-"
    return infer_country_from_state_or_port(record)


def move_misplaced_values(record):
    if record["Country"] == "-" and record["Extracted_State"] in KNOWN_COUNTRIES:
        record["Country"] = record["Extracted_State"]
        record["Extracted_State"] = "-"

    if record["Extracted_State"] == "-" and record["Extracted_City"] in US_STATES:
        record["Extracted_State"] = record["Extracted_City"]
        record["Extracted_City"] = "-"

    county_value = record["Extracted_City"]
    if (
        record["Extracted_County"] == "-"
        and county_value != "-"
        and county_value.lower().endswith(COUNTY_HINTS)
    ):
        record["Extracted_County"] = county_value
        record["Extracted_City"] = "-"

    if record["Extracted_City"] == "-" and record["Extracted_County"] != "-":
        county_parts = record["Extracted_County"].split(",")
        lead = county_parts[0].strip()
        if lead and lead not in US_STATES and not lead.lower().endswith(COUNTY_HINTS):
            record["Extracted_City"] = lead
            record["Extracted_County"] = "-"

    if record["Arrival_Port_Country"] == "-" and record["Arrival_Port"] in ARRIVAL_PORT_COUNTRIES:
        record["Arrival_Port_Country"] = ARRIVAL_PORT_COUNTRIES[record["Arrival_Port"]]

    return record


def coerce_record_types(record, source_row):
    record["ID"] = normalize_output_value(record["ID"])
    record["Book"] = normalize_output_value(record["Book"])
    record["Notes"] = normalize_value(source_row.get("Notes", record["Notes"]))
    record["Ship_Notes"] = normalize_value(source_row.get("Ship_Notes", record["Ship_Notes"]))
    record["Ref_Page"] = record["Ref_Page"] if is_numeric_string(record["Ref_Page"]) else "-"
    record["Birthdate"] = infer_birth_year(record["Notes"], record["Birthdate"])
    record["Departure_Date"] = record["Departure_Date"] if is_numeric_string(record["Departure_Date"]) else "-"
    record["Gender"] = normalize_gender(record["Gender"], record["Notes"])
    record["Race"] = normalize_race(record["Race"])
    record["Ethnicity"] = normalize_ethnicity(record["Race"], record["Ethnicity"])
    record["First_Name"] = clean_first_name(record["First_Name"])
    record["Surname"] = clean_surname(record["Surname"])
    for key in ["Father_FirstName", "Mother_FirstName"]:
        record[key] = clean_first_name(record[key])
    for key in ["Father_Surname", "Mother_Surname", "Commander", "Enslaver"]:
        record[key] = compact_name(record[key])
    return record


def apply_source_consistency(record):
    if record["Book"] != "-" and record["Primary_Source_1"] in {"-", ""}:
        record["Primary_Source_1"] = record["Book"]
    elif record["Book"] != "-" and record["Primary_Source_1"] != "-":
        book = record["Book"].lower()
        source = record["Primary_Source_1"].lower()
        if book not in source and source not in book:
            record["Primary_Source_1"] = record["Book"]
    return record


def apply_voyage_repairs(record):
    ship_notes = record["Ship_Notes"]
    if record["Commander"] == "-":
        record["Commander"] = infer_commander(ship_notes)
    if record["Arrival_Port"] == "-":
        record["Arrival_Port"] = infer_arrival_port(ship_notes)
    if record["Arrival_Port"] in ARRIVAL_PORT_COUNTRIES:
        record["Arrival_Port_Country"] = ARRIVAL_PORT_COUNTRIES[record["Arrival_Port"]]
    if record["Ship_Name"] != "-" and ship_notes != "-" and record["Ship_Name"].lower() not in ship_notes.lower():
        ship_match = re.search(r"\bship\s+([A-Z][A-Za-z'.-]+)", ship_notes, flags=re.IGNORECASE)
        if ship_match:
            record["Ship_Name"] = normalize_output_value(ship_match.group(1))
    return record


def post_process_record(output_row, source_row):
    source_record = {
        col: normalize_value(source_row.get(col, "-"))
        for col in VALIDATED_COLUMNS
    }
    model_record = {
        col: normalize_output_value(output_row[idx] if idx < len(output_row) else "-")
        for idx, col in enumerate(VALIDATED_COLUMNS)
    }

    # Spreadsheet values are the base truth. The model should only supplement
    # blanks or reinforce a value that is already present.
    record = dict(source_record)
    for col in LLM_TARGET_COLUMNS:
        source_value = source_record[col]
        model_value = model_record[col]
        if source_value == "-" and model_value != "-":
            record[col] = model_value
        elif source_value != "-" and model_value == source_value:
            record[col] = model_value

    record = move_misplaced_values(record)
    record = coerce_record_types(record, source_row)
    record = apply_geography_repairs(record)
    record = apply_voyage_repairs(record)
    record = validate_commander(record)

    # If the model changed any non-target column values, restore the original Excel values.
    for col in VALIDATED_COLUMNS:
        if col not in LLM_TARGET_COLUMNS and record[col] != source_record[col]:
            record[col] = source_record[col]

    record = apply_source_consistency(record)
    return [record[col] for col in VALIDATED_COLUMNS]


def load_county_lookup():
    if not os.path.exists(COUNTY_LOOKUP_FILE):
        return None
    try:
        return pd.read_excel(COUNTY_LOOKUP_FILE)
    except Exception:
        return None


def build_lookup_coordinate_map(df_lookup):
    if df_lookup is None:
        return {}
    columns = {col.lower(): col for col in df_lookup.columns}
    county_col = columns.get("county")
    state_col = columns.get("state")
    lat_col = columns.get("lat")
    lon_col = columns.get("lng") or columns.get("lon") or columns.get("longitude")
    if not all([county_col, state_col, lat_col, lon_col]):
        return {}

    lookup = {}
    for _, row in df_lookup.iterrows():
        county = normalize_output_value(row.get(county_col, "-"))
        state = normalize_output_value(row.get(state_col, "-"))
        lat = normalize_value(row.get(lat_col, "-"))
        lon = normalize_value(row.get(lon_col, "-"))
        if county != "-" and state != "-" and lat != "-" and lon != "-":
            lookup[(county.lower(), state.lower())] = f"{lat}, {lon}"
    return lookup


def get_global_coordinate_map(df, county_lookup_map):
    if geolocator is None:
        return {}

    dep_mask = df["Departure_Coordinates"].isin(["", "-", "nan"]) | df["Departure_Coordinates"].isna()
    arr_mask = df["Arrival_Coordinates"].isin(["", "-", "nan"]) | df["Arrival_Coordinates"].isna()
    subset = df[dep_mask | arr_mask]

    unique_dep = subset[["Extracted_City", "Extracted_County", "Extracted_State", "Country"]].drop_duplicates()
    unique_dep.columns = ["City", "County", "State", "Country"]

    unique_arr = subset[["Arrival_Port", "Arrival_Port_Country"]].drop_duplicates()
    unique_arr.columns = ["City", "Country"]
    unique_arr["County"] = "-"
    unique_arr["State"] = "-"

    combined = pd.concat([unique_dep, unique_arr], ignore_index=True).drop_duplicates()
    coord_map = {}

    for _, row in combined.iterrows():
        city = normalize_value(row["City"])
        county = normalize_value(row["County"])
        state = normalize_value(row["State"])
        country = normalize_value(row["Country"])
        loc_key = (city, county, state, country)

        county_key = (county.lower(), state.lower())
        if county != "-" and state != "-" and county_key in county_lookup_map:
            coord_map[loc_key] = county_lookup_map[county_key]
            continue

        geo_parts = [part for part in [city, county, state, country] if part != "-"]
        query = ", ".join(geo_parts)
        if not query or query in {"Canada", "United States"}:
            coord_map[loc_key] = "-"
            continue

        try:
            time.sleep(GEOPY_REQUEST_DELAY)
            location = geolocator.geocode(query, timeout=10)
            coord_map[loc_key] = f"{location.latitude}, {location.longitude}" if location else "-"
        except Exception:
            coord_map[loc_key] = "-"

    return coord_map


def assign_coordinates(df):
    if geolocator is None:
        print("Post-processing coordinates skipped: geopy/Nominatim unavailable.")
        return df

    print("Post-processing coordinates with geopy after model extraction...")
    county_lookup_df = load_county_lookup()
    county_lookup_map = build_lookup_coordinate_map(county_lookup_df)
    location_cache = get_global_coordinate_map(df, county_lookup_map)

    def assign_dep(row):
        existing = normalize_value(row.get("Departure_Coordinates", "-"))
        if existing != "-":
            return existing
        return location_cache.get(
            (
                normalize_value(row.get("Extracted_City", "-")),
                normalize_value(row.get("Extracted_County", "-")),
                normalize_value(row.get("Extracted_State", "-")),
                normalize_value(row.get("Country", "-")),
            ),
            "-",
        )

    def assign_arr(row):
        existing = normalize_value(row.get("Arrival_Coordinates", "-"))
        if existing != "-":
            return existing
        arrival_key = (
            normalize_value(row.get("Arrival_Port", "-")),
            "-",
            "-",
            normalize_value(row.get("Arrival_Port_Country", "-")),
        )
        arrival_value = location_cache.get(arrival_key, "-")
        if arrival_value != "-":
            return arrival_value
        return location_cache.get(
            (
                normalize_value(row.get("Extracted_City", "-")),
                normalize_value(row.get("Extracted_County", "-")),
                normalize_value(row.get("Extracted_State", "-")),
                normalize_value(row.get("Country", "-")),
            ),
            "-",
        )

    df["Departure_Coordinates"] = df.apply(assign_dep, axis=1)
    df["Arrival_Coordinates"] = df.apply(assign_arr, axis=1)
    return df


# --- 5. EXECUTION ---

def main():
    if not os.path.exists(INPUT_FILE):
        print(f"File {INPUT_FILE} not found.")
        return

    df = pd.read_excel(INPUT_FILE)
    last_ship = {k: "-" for k in ["Ship_Name", "Commander", "Arrival_Port", "Arrival_Port_Country", "Ship_Notes"]}
    ship_context_cache = {}
    final_rows = []

    print(f"Starting audit with {MODEL_NAME}. Rules: 8K TPM / 30 RPM / 200K Daily.")
    tracker.print_limit_status("Starting Limits")

    for idx, row in tqdm(df.iterrows(), total=len(df)):
        if tracker.get_daily_usage() >= DAILY_TOKEN_LIMIT:
            print("\nDaily token quota reached. Stopping.")
            break
        if DAILY_REQUEST_LIMIT is not None and tracker.get_daily_request_count() >= DAILY_REQUEST_LIMIT:
            print("\nDaily request quota reached. Stopping.")
            break

        if idx == 0 or idx % 25 == 0:
            tracker.print_limit_status(f"Row {idx + 1} Limits")

        while True:
            rpm, tpm = tracker.get_window_stats(60)
            if rpm < RPM_LIMIT and tpm < (TPM_LIMIT - 1200):
                break
            print(f"\r[Rate Limit] TPM: {tpm}/{TPM_LIMIT} | RPM: {rpm}/{RPM_LIMIT}. Waiting...", end="")
            time.sleep(2)

        row_dict = row.to_dict()
        if normalize_value(row_dict.get("Ship_Name", "-")) == "-":
            for key in last_ship:
                row_dict[key] = last_ship[key]
        row_dict = apply_cached_ship_context(row_dict, ship_context_cache)
        row_dict = apply_local_rule_engine(row_dict)

        try:
            time.sleep(REQUEST_DELAY)
            res = client.chat.completions.create(
                model=MODEL_NAME,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": get_validation_prompt(row_dict)},
                ],
                temperature=0,
                max_completion_tokens=1500,
                top_p=1,
                reasoning_effort="medium",
                stream=False,
            )
            tracker.log_request(res.usage.prompt_tokens, res.usage.completion_tokens)
            tracker.print_limit_status(f"After row {idx + 1}")
            clean_data = post_process_record(parse_output(res.choices[0].message.content), row_dict)

            if all(x == "-" for x in clean_data) and normalize_value(row.get("ID")) != "-":
                clean_data = post_process_record(
                    [normalize_value(row_dict.get(col, row.get(col, "-"))) for col in VALIDATED_COLUMNS],
                    row_dict,
                )

            final_rows.append(clean_data)
            last_ship.update({
                "Ship_Name": clean_data[4],
                "Ship_Notes": clean_data[6],
                "Arrival_Port": clean_data[21],
                "Arrival_Port_Country": clean_data[22],
                "Commander": clean_data[29],
            })
            update_ship_context_cache(ship_context_cache, {
                "Ship_Notes": clean_data[6],
                "Ship_Name": clean_data[4],
                "Commander": clean_data[29],
                "Arrival_Port": clean_data[21],
                "Arrival_Port_Country": clean_data[22],
            })

        except Exception as e:
            if "429" in str(e) or "rate_limit" in str(e).lower():
                print("\nRate limit hit. Cooling down for 20s...")
                time.sleep(20)
            fallback_row = post_process_record(
                [normalize_value(row_dict.get(col, row.get(col, "-"))) for col in VALIDATED_COLUMNS],
                row_dict,
            )
            final_rows.append(fallback_row)
            update_ship_context_cache(ship_context_cache, {
                "Ship_Notes": fallback_row[6],
                "Ship_Name": fallback_row[4],
                "Commander": fallback_row[29],
                "Arrival_Port": fallback_row[21],
                "Arrival_Port_Country": fallback_row[22],
            })

    out_df = pd.DataFrame(final_rows, columns=VALIDATED_COLUMNS)
    print("Model extraction complete. Starting coordinate enrichment...")
    out_df = assign_coordinates(out_df)
    out_df.to_excel(OUTPUT_FILE, index=False)
    tracker.print_session_summary()
    print(f"Process complete. Output saved to: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
