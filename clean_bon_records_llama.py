import pandas as pd
import re
import time
import os
import json
from datetime import datetime, timedelta
from tqdm import tqdm
from dotenv import load_dotenv
from groq import Groq

# --- 1. CONFIGURATION ---
load_dotenv()
client = Groq(api_key=os.getenv("GROQ_API_KEY"))

MODEL_NAME = "llama-3.1-8b-instant"
EXPECTED_OUTPUT_COLUMNS = 33
BIRTH_BASE_YEAR = 1783

# RATE LIMITS (6K TPM, 30 RPM)
TPM_LIMIT = 5800           # Slightly under 6000 for safety
RPM_LIMIT = 28             # Slightly under 30 for safety
DAILY_TOKEN_LIMIT = 485000 
REQUEST_DELAY = 2.1        # Minimum delay to respect RPM (60s/28req)

INPUT_FILE = 'Consolidated_Directory_v12_subset.xlsx' 
OUTPUT_FILE = 'Validated_Records_Final.xlsx'
USAGE_LOG_FILE = 'groq_usage_log.json'

# PRICING FOR SUMMARY (Per 1M Tokens)
INPUT_COST_1M = 0.15
OUTPUT_COST_1M = 0.60

VALIDATED_COLUMNS = [
    "ID", "Book", "First_Name", "Surname", "Ship_Name", "Notes", "Ship_Notes",
    "Birthdate", "Gender", "Race", "Ethnicity", "Origin", "Extracted_City", 
    "Extracted_County", "Extracted_State", "Extracted_Area", "Country", 
    "Departure_Coordinates", "Origination_Port", "Departure_Port", 
    "Departure_Date", "Arrival_Port", "Arrival_Port_Country", "Arrival_Coordinates", 
    "Father_FirstName", "Father_Surname", "Mother_FirstName", "Mother_Surname", 
    "Ref_Page", "Commander", "Enslaver", "Primary_Source_1", "Primary_Source_2"
]
COLUMN_INDEX = {name: idx for idx, name in enumerate(VALIDATED_COLUMNS)}
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

# --- 2. USAGE & RATE LIMIT TRACKER ---

class UsageTracker:
    def __init__(self, log_file):
        self.log_file = log_file
        self.session_in_tokens = 0
        self.session_out_tokens = 0
        if not os.path.exists(self.log_file):
            with open(self.log_file, 'w') as f: json.dump([], f)

    def log_request(self, in_tokens, out_tokens):
        self.session_in_tokens += in_tokens
        self.session_out_tokens += out_tokens
        now = datetime.now()
        entry = {
            "timestamp": now.isoformat(), 
            "input_tokens": in_tokens, 
            "output_tokens": out_tokens,
            "total_tokens": in_tokens + out_tokens
        }
        try:
            with open(self.log_file, 'r+') as f:
                data = json.load(f)
                data.append(entry)
                f.seek(0)
                json.dump(data, f)
        except: pass

    def get_window_stats(self, seconds=60):
        """Returns (request_count, total_tokens) for the last X seconds."""
        cutoff = datetime.now() - timedelta(seconds=seconds)
        try:
            with open(self.log_file, 'r') as f:
                data = json.load(f)
                window_data = [e for e in data if datetime.fromisoformat(e['timestamp']) > cutoff]
                req_count = len(window_data)
                total_tokens = sum(e['total_tokens'] for e in window_data)
                return req_count, total_tokens
        except: return 0, 0

    def get_daily_usage(self):
        """Returns total tokens used in last 24 hours."""
        cutoff = datetime.now() - timedelta(hours=24)
        try:
            with open(self.log_file, 'r') as f:
                data = json.load(f)
                daily_data = [e for e in data if datetime.fromisoformat(e['timestamp']) > cutoff]
                return sum(e['total_tokens'] for e in daily_data)
        except: return 0

    def print_session_summary(self):
        cost = ((self.session_in_tokens / 1_000_000) * INPUT_COST_1M) + \
               ((self.session_out_tokens / 1_000_000) * OUTPUT_COST_1M)
        print("\n" + "="*45)
        print("📊 FINAL CLEANING SUMMARY")
        print(f"Total Tokens:   {self.session_in_tokens + self.session_out_tokens:,}")
        print(f" - Input:       {self.session_in_tokens:,}")
        print(f" - Output:      {self.session_out_tokens:,}")
        print(f"Total Cost:     ${cost:.4f}")
        print("="*45)

tracker = UsageTracker(USAGE_LOG_FILE)

# --- 3. PROMPT GENERATOR ---

def get_validation_prompt(current_values_dict):
    normalized_values = {
        col: normalize_value(current_values_dict.get(col, "-"))
        for col in VALIDATED_COLUMNS
    }
    current_values_str = "\n".join([f"{k}: {normalized_values.get(k, '-')}" for k in VALIDATED_COLUMNS])

    return f"""### ROLE
Historical data validator for Book of Negroes records (1783).

### OBJECTIVE
Validate the CURRENT RECORD and return exactly {EXPECTED_OUTPUT_COLUMNS} pipe-separated values.
Use Notes and Ship_Notes only when a rule below allows it.
If a value is missing, invalid, or unsupported, return '-'.
No headers, labels, comments, or markdown.

### CORE RULES
- Preserve as-is unless invalid: ID, Book, Notes, Ship_Notes, Ref_Page, Primary_Source_1, Primary_Source_2.
- Do not invent facts.
- Output values only, in the exact column order shown below.

### FIELD RULES
- ID: numeric or alphanumeric only.
- Book: source reference; should match Primary_Source_1 or Primary_Source_2.
- First_Name: given name only.
- Surname: family name only; no Jr./Sr.
- Ref_Page: numeric only.

- Gender: only Male, Female, Child Male, Child Female, Unknown, or '-'.
- Race: only Black, White, Mulatto, Quadroon, Mestizo, Indigenous, Asian, Unknown, or '-'.
- Ethnicity:
  - Black -> African American or '-'
  - Mulatto/Quadroon -> Mixed Race
  - Indigenous/White -> specific descriptor or '-'
- Origin: specific place only.
- Birthdate: YYYY only; if age is given, use {BIRTH_BASE_YEAR} - age.

- Origination_Port / Departure_Port / Arrival_Port: specific port names only.
- Departure_Coordinates / Arrival_Coordinates: decimal `lat, long` only.
- Extracted_City: city/town only.
- Extracted_County: county/region only.
- Extracted_State: valid US state only.
- Extracted_Area: non-city/non-county/non-state area only.
- Country and Arrival_Port_Country: sovereign countries only and must match geography.

- Father/Mother name fields: names only.
- Ship_Name: vessel name supported by Ship_Notes.
- Commander: captain/master from Ship_Notes only.
- Departure_Date: numeric year/date only, typically 1783.
- Enslaver: owner/claimant from Notes only when supported.

### REQUIRED CHECKS
- Move state from Extracted_City to Extracted_State.
- Move county from Extracted_City to Extracted_County.
- Move city from Extracted_County to Extracted_City.
- Move country from Extracted_State to Country.
- If Birthdate, Ref_Page, Departure_Date, or coordinates are not numeric in valid format, return '-'.
- If Race is Mulatto or Quadroon, Ethnicity must be Mixed Race.
- If Race is Black, Ethnicity must be African American or '-'.
- If Gender is invalid, return Unknown or '-'.
- If Ship_Notes clearly gives Commander or Arrival_Port and those fields are '-', populate them.
- If Arrival_Port_Country conflicts with Arrival_Port, correct it.
- If Book and Primary_Source_1 conflict, prefer supported consistent source values.

### CURRENT RECORD
{current_values_str}

### OUTPUT ORDER
ID | Book | First_Name | Surname | Ship_Name | Notes | Ship_Notes | Birthdate | Gender | Race | Ethnicity | Origin | Extracted_City | Extracted_County | Extracted_State | Extracted_Area | Country | Departure_Coordinates | Origination_Port | Departure_Port | Departure_Date | Arrival_Port | Arrival_Port_Country | Arrival_Coordinates | Father_FirstName | Father_Surname | Mother_FirstName | Mother_Surname | Ref_Page | Commander | Enslaver | Primary_Source_1 | Primary_Source_2

### RESPONSE FORMAT
Return exactly {EXPECTED_OUTPUT_COLUMNS} pipe-separated values only.
"""

# --- 4. ENGINE ---

def normalize_value(value):
    if pd.isna(value):
        return "-"
    text = str(value).strip()
    return text if text and text.lower() != "nan" else "-"

def parse_output(raw):
    lines = [l.strip() for l in raw.splitlines() if "|" in l]
    if not lines:
        return ["-"] * EXPECTED_OUTPUT_COLUMNS
    data_line = max(lines, key=lambda l: l.count("|"))
    parts = [p.strip() if p.strip() else "-" for p in data_line.split("|")]

    # Header protection: skip if model returned the column names
    if "First_Name" in parts or "Ship_Notes" in parts:
        return ["-"] * EXPECTED_OUTPUT_COLUMNS
    return [normalize_output_value(part) for part in (parts + ["-"] * EXPECTED_OUTPUT_COLUMNS)[:EXPECTED_OUTPUT_COLUMNS]]

def normalize_output_value(value):
    value = normalize_value(value)
    if value == "-":
        return value
    return re.sub(r"\s+", " ", value)

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
        "unknown": "Unknown"
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
    patterns = [
        r"(?:captain|capt\.?|master|commander)\s+([A-Z][A-Za-z'.-]+(?:\s+[A-Z][A-Za-z'.-]+)+)",
        r"commanded by\s+([A-Z][A-Za-z'.-]+(?:\s+[A-Z][A-Za-z'.-]+)+)"
    ]
    for pattern in patterns:
        match = re.search(pattern, ship_notes, flags=re.IGNORECASE)
        if match:
            return compact_name(match.group(1))
    return "-"

def infer_arrival_port(ship_notes):
    ship_notes = normalize_value(ship_notes)
    patterns = [
        r"bound for\s+([A-Za-z'. -]+)",
        r"for\s+([A-Za-z'. -]+)$",
        r"to\s+([A-Za-z'. -]+)$"
    ]
    for pattern in patterns:
        match = re.search(pattern, ship_notes, flags=re.IGNORECASE)
        if match:
            port = normalize_output_value(match.group(1).strip(" .,;:"))
            return port if not any(ch.isdigit() for ch in port) else "-"
    return "-"

def infer_country_from_state_or_port(record):
    state = record["Extracted_State"]
    arrival_port = record["Arrival_Port"]
    origin = "United States" if state in US_STATES else "-"
    if arrival_port in ARRIVAL_PORT_COUNTRIES:
        record["Arrival_Port_Country"] = ARRIVAL_PORT_COUNTRIES[arrival_port]
    if origin != "-" and record["Country"] == "-":
        record["Country"] = origin
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

    if county in US_STATES:
        record["Extracted_State"] = county
        record["Extracted_County"] = "-"

    if state in KNOWN_COUNTRIES:
        record["Country"] = state
        record["Extracted_State"] = "-"

    if record["Departure_Coordinates"] != "-" and not is_coordinate_string(record["Departure_Coordinates"]):
        record["Departure_Coordinates"] = "-"
    if record["Arrival_Coordinates"] != "-" and not is_coordinate_string(record["Arrival_Coordinates"]):
        record["Arrival_Coordinates"] = "-"
    return infer_country_from_state_or_port(record)

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
    record = {
        col: normalize_output_value(output_row[idx] if idx < len(output_row) else "-")
        for idx, col in enumerate(VALIDATED_COLUMNS)
    }
    for preserved in ["ID", "Book", "Notes", "Ship_Notes", "Ref_Page", "Primary_Source_1", "Primary_Source_2"]:
        source_value = normalize_value(source_row.get(preserved, "-"))
        if source_value != "-":
            record[preserved] = source_value
    record = coerce_record_types(record, source_row)
    record = apply_geography_repairs(record)
    record = apply_voyage_repairs(record)
    record = apply_source_consistency(record)
    return [record[col] for col in VALIDATED_COLUMNS]

def main():
    if not os.path.exists(INPUT_FILE):
        print(f"Error: {INPUT_FILE} not found.")
        return
    
    df = pd.read_excel(INPUT_FILE)
    last_ship = {k: "-" for k in ["Ship_Name", "Commander", "Arrival_Port", "Arrival_Port_Country", "Ship_Notes"]}
    final_rows = []

    print(f"Starting audit. Rules: 6K TPM / 30 RPM / 500K Daily.")

    for i, row in tqdm(df.iterrows(), total=len(df)):
        # 1. Respect Daily Limit
        if tracker.get_daily_usage() >= DAILY_TOKEN_LIMIT:
            print("\nDaily token quota reached. Stopping.")
            break

        # 2. Respect TPM & RPM (60-second sliding window)
        while True:
            rpm, tpm = tracker.get_window_stats(60)
            if rpm < RPM_LIMIT and tpm < (TPM_LIMIT - 1300): # Buffer for current req
                break
            # Status update while waiting
            print(f"\r[Rate Limit] TPM: {tpm}/{TPM_LIMIT} | RPM: {rpm}/{RPM_LIMIT}. Waiting...", end="")
            time.sleep(2)

        # 3. Persistence Logic
        row_dict = row.to_dict()
        if normalize_value(row_dict.get('Ship_Name', '-')) == '-':
            for k in last_ship: row_dict[k] = last_ship[k]

        try:
            time.sleep(REQUEST_DELAY)
            res = client.chat.completions.create(
                model=MODEL_NAME,
                messages=[{"role": "user", "content": get_validation_prompt(row_dict)}],
                temperature=0,
                max_completion_tokens=500
            )
            
            tracker.log_request(res.usage.prompt_tokens, res.usage.completion_tokens)
            clean_data = post_process_record(parse_output(res.choices[0].message.content), row_dict)
            
            # 4. Fallback if validation returned headers or empty
            if all(x == "-" for x in clean_data) and normalize_value(row.get('ID')) != '-':
                 final_rows.append(post_process_record(
                     [normalize_value(row.get(col, "-")) for col in VALIDATED_COLUMNS],
                     row_dict
                 ))
            else:
                final_rows.append(clean_data)
                # Update voyage context
                last_ship.update({
                    "Ship_Name": clean_data[4], "Ship_Notes": clean_data[6],
                    "Arrival_Port": clean_data[21], "Arrival_Port_Country": clean_data[22],
                    "Commander": clean_data[29]
                })

        except Exception as e:
            if "429" in str(e):
                print("\nRate limit hit. Cooling down for 30s...")
                time.sleep(30)
            final_rows.append(post_process_record(
                [normalize_value(row.get(col, "-")) for col in VALIDATED_COLUMNS],
                row_dict
            ))

    # Save results to Excel
    out_df = pd.DataFrame(final_rows, columns=VALIDATED_COLUMNS)
    out_df.to_excel(OUTPUT_FILE, index=False)
    
    tracker.print_session_summary()
    print(f"Success! Validated data saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
