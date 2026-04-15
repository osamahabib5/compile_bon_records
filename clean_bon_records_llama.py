import pandas as pd
import re
import time
import os
import json
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
from dotenv import load_dotenv
from groq import Groq

# --- 1. CONFIGURATION ---
load_dotenv()
client = Groq(api_key=os.getenv("GROQ_API_KEY"))

MODEL_NAME = "llama-3.1-8b-instant"

# RATE LIMIT TUNING (For 6,000 TPM limit)
MAX_WORKERS = 1           
REQUEST_DELAY = 18.0      
BIRTH_BASE_YEAR = 1783
INPUT_FILE = 'Consolidated_Directory_v12_subset.xlsx' 
OUTPUT_FILE = 'Validated_Records_Cleaned.xlsx'
USAGE_LOG_FILE = 'groq_usage_log.json'

VALIDATED_COLUMNS = [
    "ID", "Book", "First_Name", "Surname", "Ship_Name", "Notes", "Ship_Notes",
    "Birthdate", "Gender", "Race", "Ethnicity", "Origin", "Extracted_City", 
    "Extracted_County", "Extracted_State", "Extracted_Area", "Country", 
    "Departure_Coordinates", "Origination_Port", "Departure_Port", 
    "Departure_Date", "Arrival_Port", "Arrival_Port_Country", "Arrival_Coordinates", 
    "Father_FirstName", "Father_Surname", "Mother_FirstName", "Mother_Surname", 
    "Ref_Page", "Commander", "Enslaver", "Primary_Source_1", "Primary_Source_2"
]

# --- 2. USAGE TRACKING ---

class UsageTracker:
    def __init__(self, log_file):
        self.log_file = log_file
        if not os.path.exists(self.log_file):
            with open(self.log_file, 'w') as f:
                json.dump([], f)

    def log_request(self, tokens_used):
        now = datetime.now().isoformat()
        try:
            with open(self.log_file, 'r+') as f:
                data = json.load(f)
                data.append({"timestamp": now, "tokens": tokens_used})
                f.seek(0)
                json.dump(data, f)
        except: pass

    def get_24h_stats(self):
        cutoff = datetime.now() - timedelta(hours=24)
        total_tokens = 0
        total_requests = 0
        try:
            with open(self.log_file, 'r') as f:
                data = json.load(f)
                valid_data = [e for e in data if datetime.fromisoformat(e['timestamp']) > cutoff]
                total_tokens = sum(e['tokens'] for e in valid_data)
                total_requests = len(valid_data)
            with open(self.log_file, 'w') as f:
                json.dump(valid_data, f)
            return total_requests, total_tokens
        except: return 0, 0

tracker = UsageTracker(USAGE_LOG_FILE)

# --- 3. REFINED PROMPT WITH EXAMPLES ---

def build_prompt(row: pd.Series) -> str:
    current = row.to_dict()
    current_values = "\n".join([f"{k}: {current.get(k, '-')}" for k in VALIDATED_COLUMNS])
    
    return f"""### ROLE
Precision Historical Data Auditor (Book of Negroes, 1783).

### OBJECTIVE
Extract data into EXACTLY 33 pipe-separated values (|). Follow these logic patterns exactly:

### 📖 FEW-SHOT EXAMPLES:
Example 1:
Notes: Billy Williams, 35, healthy stout man, (Richard Browne). Formerly lived with Mr. Moore of Reedy Island, Caroline...
Ship_Notes: Ship Aurora bound for St. John's
Output: [ID] | [Book] | Billy | Williams | [Ship_Name] | [Notes] | [Ship_Notes] | 1748 | Male | Black | African American | - | - | Caroline | Delaware | Reedy Island | United States | 40.7128, -74.0060 | - | New York | 1783 | St. John's | Canada | 45.2733, -66.0633 | - | - | - | - | [Ref_Page] | - | Richard Browne | [Primary_Source_1] | [Primary_Source_2]

Example 2:
Notes: Barbarry Allen, 22, healthy stout wench, (Humphry Winters). Property of Humphrey Winters of New York from Virginia.
Ship_Notes: Ship Aurora bound for St. John's
Output: [ID] | [Book] | Barbarry | Allen | [Ship_Name] | [Notes] | [Ship_Notes] | 1761 | Female | Black | African American | - | - | - | Virginia | - | United States | 40.7128, -74.0060 | - | New York | 1783 | St. John's | Canada | 45.2733, -66.0633 | - | - | - | - | [Ref_Page] | - | Humphry Winters | [Primary_Source_1] | [Primary_Source_2]

### 🛑 FINAL AUDIT CHECKLIST:
1. Birthdate: Calculate as (1783 - Age). 
2. Extracted_State: US States only (e.g., Virginia). 
3. Extracted_County: NEVER put states here. If "Caroline, Virginia", County is Caroline, State is Virginia.
4. Arrival_Port_Country: Text only (Canada). NEVER put coordinates here.
5. Arrival_Coordinates: Numeric only. NEVER put text here.
6. Preservation: Copy [ID, Book, Notes, Ship_Name, Ship_Notes, Ref_Page] exactly.

---
### CURRENT DATA:
{current_values}

### TEXT SOURCE:
Notes: {current.get('Notes', '-')}
Ship_Notes: {current.get('Ship_Notes', '-')}

RESPOND WITH PIPE-SEPARATED VALUES ONLY:"""

# --- 4. PARSING & UTILITIES ---

def parse_pipe_output(raw_output: str, expected_parts: int = 33) -> list[str]:
    lines = [l.strip() for l in raw_output.splitlines() if l.count("|") >= 10]
    if not lines: return ["-"] * expected_parts
    data_line = max(lines, key=lambda l: l.count("|"))
    parts = [p.strip() if p.strip() else "-" for p in data_line.split("|")]
    return (parts + ["-"] * expected_parts)[:expected_parts]

def calculate_birth_year(age_val):
    try:
        if re.fullmatch(r"\d{4}", str(age_val)): return age_val
        match = re.search(r"\d+", str(age_val))
        return BIRTH_BASE_YEAR - int(match.group()) if match else "-"
    except: return "-"

# --- 5. EXECUTION ---

def process_row(row: pd.Series) -> list[str]:
    prompt = build_prompt(row)
    try:
        time.sleep(REQUEST_DELAY) 
        completion = client.chat.completions.create(
            model=MODEL_NAME,
            messages=[{"role": "user", "content": prompt}],
            temperature=0, 
            max_completion_tokens=400, 
            stream=False
        )
        
        tracker.log_request(completion.usage.total_tokens)
        req_24, tok_24 = tracker.get_24h_stats()
        print(f"\r[24h Stats] Requests: {req_24} | Tokens: {tok_24:,}", end="")
        
        return parse_pipe_output(completion.choices[0].message.content)
    except Exception as e:
        if "429" in str(e): time.sleep(30)
        return ["-"] * len(VALIDATED_COLUMNS)

def main():
    if not os.path.exists(INPUT_FILE): return
    df = pd.read_excel(INPUT_FILE)
    
    req_24, tok_24 = tracker.get_24h_stats()
    print(f"Starting audit. Current 24h usage: {req_24} reqs, {tok_24:,} tokens.")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        results = list(tqdm(executor.map(process_row, [row for _, row in df.iterrows()]), total=len(df)))

    for i, col in enumerate(VALIDATED_COLUMNS):
        df[col] = [r[i] if i < len(r) else "-" for r in results]

    df["Birthdate"] = df["Birthdate"].apply(calculate_birth_year)
    df.to_excel(OUTPUT_FILE, index=False)
    print(f"\nProcessing Complete. Saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()