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

# Configuration optimized for Qwen 3-32B Limits
MODEL_NAME = "qwen/qwen3-32b"
MAX_WORKERS = 2           # Keeping concurrency low to stay under 6k tokens/min
REQUEST_DELAY = 1.1       # Ensures we stay under 60 requests per minute
BIRTH_BASE_YEAR = 1783
INPUT_FILE = 'Consolidated_Directory_v12_subset.xlsx' 
OUTPUT_FILE = 'Validated_Records_Cleaned.xlsx'

VALIDATED_COLUMNS = [
    "ID", "Book", "First_Name", "Surname", "Ship_Name", "Notes", "Ship_Notes",
    "Birthdate", "Gender", "Race", "Ethnicity", "Origin", "Extracted_City", 
    "Extracted_County", "Extracted_State", "Extracted_Area", "Country", 
    "Departure_Coordinates", "Origination_Port", "Departure_Port", 
    "Departure_Date", "Arrival_Port", "Arrival_Port_Country", "Arrival_Coordinates", 
    "Father_FirstName", "Father_Surname", "Mother_FirstName", "Mother_Surname", 
    "Ref_Page", "Commander", "Enslaver", "Primary_Source_1", "Primary_Source_2"
]

# --- 2. PROMPT BUILDING ---

def build_prompt(row: pd.Series) -> str:
    """Refined prompt with strict categorical exclusion and Qwen-specific formatting."""
    current = row.to_dict()
    current_values = "\n".join([f"{k}: {current.get(k, '-')}" for k in VALIDATED_COLUMNS])
    context = f"Notes: {current.get('Notes', '-')}\nShip_Notes: {current.get('Ship_Notes', '-')}"

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
        - *Example 1*: "St. Paul's, London" -> Extracted_City: London 
        - *Example 2*: "Born free at Kingston, Jamaica" -> Extracted_City: Kingston
   - **Extracted_County**: County names only (e.g., Chesterfield, Princess Ann). No Cities/States/Country and numeric values.
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
Output: [ID] | [Book] | Billy | Williams | [Ship_Name] | [Notes] | [Ship_Notes] | 1748 | Male | Black | African American | - | - | - | Delaware | Reedy Island | United States | 40.7128, -74.0060 | - | New York | 1783 | St. John's | Canada | 45.2733, -66.0633 | - | - | - | - | [Ref_Page] | - | Richard Browne | [Primary_Source_1] | [Primary_Source_2]

Notes: Barbarry Allen, 22, healthy stout wench, (Humphry Winters). Property of Humphrey Winters of New York from Virginia.
Ship_Notes: Ship Aurora bound for St. John's
Output: [ID] | [Book] | Barbarry | Allen | [Ship_Name] | [Notes] | [Ship_Notes] | 1761 | Female | Black | African American | - | - | - | Virginia | - | United States | 40.7128, -74.0060 | - | New York | 1783 | St. John's | Canada | 45.2733, -66.0633 | - | - | - | - | [Ref_Page] | - | Humphry Winters | [Primary_Source_1] | [Primary_Source_2]

---

### CURRENT DATA (FOR PRESERVATION):
{current_values}

### TEXT SOURCE (FOR EXTRACTION):
{context}

### OUTPUT FORMAT (33 VALUES):
ID | Book | First_Name | Surname | Ship_Name | Notes | Ship_Notes | Birthdate | Gender | Race | Ethnicity | Origin | Extracted_City | Extracted_County | Extracted_State | Extracted_Area | Country | Departure_Coordinates | Origination_Port | Departure_Port | Departure_Date | Arrival_Port | Arrival_Port_Country | Arrival_Coordinates | Father_FirstName | Father_Surname | Mother_FirstName | Mother_Surname | Ref_Page | Commander | Enslaver | Primary_Source_1 | Primary_Source_2

RESPOND WITH PIPE-SEPARATED VALUES ONLY:
"""

# --- 3. PARSING & UTILITIES ---

def parse_pipe_output(raw_output: str, expected_parts: int = 33) -> list[str]:
    if not raw_output:
        return ["-"] * expected_parts
    lines = raw_output.strip().split('\n')
    # Pick the line with the most pipes to bypass any AI chatter
    extracted_line = max(lines, key=lambda l: l.count("|"))
    parts = [p.strip() if p.strip() else "-" for p in extracted_line.split("|")]
    while len(parts) < expected_parts:
        parts.append("-")
    return parts[:expected_parts]

def calculate_birth_year(age_val):
    try:
        if re.fullmatch(r"\d{4}", str(age_val)): return age_val
        match = re.search(r"\d+", str(age_val))
        return BIRTH_BASE_YEAR - int(match.group()) if match else "-"
    except: return "-"

# --- 4. EXECUTION ---

def process_row(row: pd.Series) -> list[str]:
    prompt = build_prompt(row)
    try:
        time.sleep(REQUEST_DELAY) # Rate limit pacing
        completion = client.chat.completions.create(
            model=MODEL_NAME,
            messages=[{"role": "user", "content": prompt}],
            temperature=0, 
            max_completion_tokens=400, # Lower tokens = safer for your 6k/min limit
            top_p=0.95,
            stream=False
        )
        return parse_pipe_output(completion.choices[0].message.content)
    except Exception as e:
        if "rate_limit" in str(e).lower():
            time.sleep(10) # Back off if limited
        return ["-"] * len(VALIDATED_COLUMNS)

def main():
    if not os.path.exists(INPUT_FILE):
        print(f"File {INPUT_FILE} not found.")
        return

    df = pd.read_excel(INPUT_FILE)
    print(f"Auditing {len(df)} records using {MODEL_NAME}...")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        results = list(tqdm(executor.map(process_row, [row for _, row in df.iterrows()]), total=len(df)))

    # Map results back to DataFrame
    for i, col in enumerate(VALIDATED_COLUMNS):
        df[col] = [r[i] if i < len(r) else "-" for r in results]

    df["Birthdate"] = df["Birthdate"].apply(calculate_birth_year)
    df.to_excel(OUTPUT_FILE, index=False)
    print(f"Process complete. Output saved to: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()