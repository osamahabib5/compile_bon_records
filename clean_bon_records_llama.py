import pandas as pd
import re
import time
import os
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
from dotenv import load_dotenv
from groq import Groq

# --- 1. CONFIGURATION & CONSTANTS ---
load_dotenv()
client = Groq(api_key=os.getenv("GROQ_API_KEY"))

MODEL_NAME = "llama-3.1-8b-instant"
MAX_WORKERS = 1  
REQUEST_DELAY_SECONDS = 2.0
BIRTH_BASE_YEAR = 1783
INPUT_FILE = 'Consolidated_Directory_v12_subset.xlsx' 
OUTPUT_FILE = 'Validated_Records_Cleaned.xlsx'

# The 33 columns schema
VALIDATED_COLUMNS = [
    "ID", "Book", "First_Name", "Surname", "Ship_Name", "Notes", "Ship_Notes",
    "Birthdate", "Gender", "Race", "Ethnicity", "Origin", "Extracted_City", 
    "Extracted_County", "Extracted_State", "Extracted_Area", "Country", 
    "Departure_Coordinates", "Origination_Port", "Departure_Port", 
    "Departure_Date", "Arrival_Port", "Arrival_Port_Country", "Arrival_Coordinates", 
    "Father_FirstName", "Father_Surname", "Mother_FirstName", "Mother_Surname", 
    "Ref_Page", "Commander", "Enslaver", "Primary_Source_1", "Primary_Source_2"
]

# --- 2. HELPER FUNCTIONS ---

def build_prompt(row: pd.Series) -> str:
    """Build refined prompt with strict 33-column mapping and validation logic."""
    current = row.to_dict()
    current_values = "\n".join([f"{k}: {current.get(k, '-')}" for k in VALIDATED_COLUMNS])

    context = (
        f"Notes: {current.get('Notes', '-')}\n"
        f"Ship_Notes: {current.get('Ship_Notes', '-')}"
    )

    return f"""### ROLE
You are a precision-oriented historical data auditor for the Book of Negroes (1783).

### OBJECTIVE
Clean and enrich historical records. You must return EXACTLY 33 pipe-separated values (|). 

### EXTRACTION & VALIDATION RULES:
1. **STRICT PRESERVATION (DO NOT MODIFY)**: 
   For [ID, Book, Notes, Ship_Name, Ship_Notes, Ref_Page, Primary_Source_1, Primary_Source_2], use the EXACT value provided in "CURRENT DATA". Do not fix typos or update these from the text.

2. **IDENTITY LOGIC**:
   - "Mulatto" -> Race: Mulatto | Ethnicity: Mixed Race.
   - "Quadroon" -> Race: Quadroon | Ethnicity: Mixed Race.
   - Mixed Heritage (e.g., "Indian & Span", "Mother an Indian") -> Origin: [Heritages] | Ethnicity: Mixed Race.
   - Example: Andrew Hilton (mulatto, mother Indian) -> Race: Mulatto | Ethnicity: Mixed Race | Origin: Indian.

3. **AGE & TEMPORAL DATA**:
   - **Birthdate**: Extract age from Notes. Return as: ({BIRTH_BASE_YEAR} - Age). Result must be a 4-digit year.
   - **Departure_Date**: Must be '1783' for all records. No non-numeric values allowed (like 'New York')
   - **Departure_Coordinates/Arrival_Coordinates**: Return 'latitude, longitude' (numbers and decimals only). Else '-'. No text allowed.

4. **GEOGRAPHIC CONSTRAINTS**:
   - **Extracted_State**: Valid US State only (e.g., Virginia). Else '-'. No numeric values. Example: "From Virginia" -> Extracted_State: Virginia.
   - **Extracted_City**: City/Town names only (e.g., Philadelpha). No Counties/States/Country.No numeric values.
        - *Example 1*: "St. Paul's, London" -> Extracted_City: London | Country: United Kingdom.
        - *Example 2*: "Born free at Kingston, Jamaica" -> Extracted_City: Kingston | Country: Jamaica.
   - **Extracted_County**: County names only (e.g., Chesterfield). No Cities/States/Country allowed (like 'Virginia', 'United States','New York'). No numeric values.
   - **Extracted_Area**: US Islands or specific areas (e.g., Reedy Island). No Cities/States/Country/Counties.No numeric values.
   - **Country**: 
        - Default to 'United States' if a US state/city is identified. No numeric values allowed.
        - Set to 'United Kingdom' for London/English parishes.
        - Set to 'Jamaica' for Kingston/Jamaican locations.
   - **Arrival_Port_Country**: Default to 'Canada' if Arrival_Port is in Nova Scotia, St. John's, or similar areas. No numeric or non-country values allowed
   - **Arrival_Port**: No numeric values allowed. Extract from Ship_Notes only. like St. John's, Port Roseway, etc.
   - **Commander/Enslaver**: Valid personal names only. No locations, or other text or numeric values.

5. **SOURCE ATTRIBUTION**:
   - **Ship Data**: Commander, Arrival_Port, and Arrival_Port_Country must come ONLY from Ship_Notes.

6. **STRICT LIMITATION**: 
   Do not guess. If information is not explicitly in the Text Source, use '-'.

### EXAMPLE DATA:
Notes: Billy Williams, 35, healthy stout man, (Richard Browne). Formerly lived with Mr. Moore of Reedy Island, Caroline...
Ship_Notes: Ship Aurora bound for St. John's
Output: [ID] | [Book] | Billy | Williams | [Ship_Name] | [Notes] | [Ship_Notes] | 1748 | Male | Black | African American | - | - | - | Delaware | Reedy Island, Caroline | United States | 40.7128, -74.0060 | - | New York | 1783 | St. John's | Canada | 45.2733, -66.0633 | - | - | - | - | [Ref_Page] | - | Richard Browne | [Primary_Source_1] | [Primary_Source_2]

---

### CURRENT DATA (FOR PRESERVATION):
{current_values}

### TEXT SOURCE (FOR EXTRACTION):
{context}

### OUTPUT FORMAT (33 VALUES):
ID | Book | First_Name | Surname | Ship_Name | Notes | Ship_Notes | Birthdate | Gender | Race | Ethnicity | Origin | Extracted_City | Extracted_County | Extracted_State | Extracted_Area | Country | Departure_Coordinates | Origination_Port | Departure_Port | Departure_Date | Arrival_Port | Arrival_Port_Country | Arrival_Coordinates | Father_FirstName | Father_Surname | Mother_FirstName | Mother_Surname | Ref_Page | Commander | Enslaver | Primary_Source_1 | Primary_Source_2

RESPOND WITH PIPE-SEPARATED VALUES ONLY:
"""

def parse_pipe_output(raw_output: str, expected_parts: int = 33) -> list[str]:
    """Parse model output into a fixed-size list of 33 parts."""
    if not raw_output:
        return ["-"] * expected_parts

    lines = raw_output.splitlines()
    # Find line with most pipes to avoid preamble
    extracted_line = max(lines, key=lambda l: l.count("|")).strip()
    extracted_line = re.sub(r"^\s*[A-Za-z_ ]+:\s*", "", extracted_line).strip()
    
    parts = [p.strip() if p.strip() else "-" for p in extracted_line.split("|")]

    while len(parts) < expected_parts:
        parts.append("-")
    return parts[:expected_parts]

def calculate_birth_year(age_value: str, base_year: int = BIRTH_BASE_YEAR):
    """Secondary check to ensure birth year is formatted correctly."""
    try:
        if re.fullmatch(r"\d{4}", str(age_value)):
            return age_value
        match = re.search(r"\d+", str(age_value))
        if not match: return "-"
        return base_year - int(match.group())
    except:
        return "-"

# --- 3. CORE PROCESSING ---

def validate_and_fix_row(row: pd.Series) -> list[str]:
    """Execute API call for a single row."""
    prompt = build_prompt(row)
    try:
        time.sleep(REQUEST_DELAY_SECONDS)
        completion = client.chat.completions.create(
            model=MODEL_NAME,
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
            max_completion_tokens=900,
        )
        raw_output = completion.choices[0].message.content.strip()
        return parse_pipe_output(raw_output, expected_parts=len(VALIDATED_COLUMNS))
    except Exception as exc:
        print(f"Error processing row {row.get('ID', 'unknown')}: {exc}")
        return ["-"] * len(VALIDATED_COLUMNS)

def main():
    if not os.path.exists(INPUT_FILE):
        print(f"Error: {INPUT_FILE} not found.")
        return

    df = pd.read_excel(INPUT_FILE)
    print(f"Auditing {len(df)} records with strict 33-column schema...")

    start_time = time.time()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        results = list(tqdm(
            executor.map(validate_and_fix_row, [row for _, row in df.iterrows()]),
            total=len(df),
            desc="Auditing"
        ))

    # Apply results back to the DataFrame
    for index, col_name in enumerate(VALIDATED_COLUMNS):
        df[col_name] = [r[index] if index < len(r) else "-" for r in results]

    print("Finalizing Birth Year validation...")
    df["Birthdate"] = df["Birthdate"].apply(calculate_birth_year)

    df.to_excel(OUTPUT_FILE, index=False)
    
    elapsed = round(time.time() - start_time, 2)
    print(f"\n✨ SUCCESS. Results saved to {OUTPUT_FILE}")
    print(f"Total time: {elapsed}s | Average: {elapsed/len(df):.2f}s per record")

if __name__ == "__main__":
    main()