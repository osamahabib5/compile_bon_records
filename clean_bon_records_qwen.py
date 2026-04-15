import pandas as pd
import re
import time
import os
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
from dotenv import load_dotenv
from groq import Groq

# --- 1. Initialization ---
load_dotenv()

# Initialize Groq client
client = Groq(api_key=os.getenv("GROQ_API_KEY"))

def ollama_validate_and_fix(row):
    """
    Extracts 32 columns using Qwen 3-32B via Groq API.
    """
    current_data = row.to_dict()
    context = (
        f"Notes: {current_data.get('Notes', '-')}\n"
        f"Ship_Notes: {current_data.get('Ship_Notes', '-')}"
    )
    
    # REFINED PROMPT: 32 Columns with Specific Identity & Batch Logic
    # REFINED PROMPT: 32 Columns with Specific Identity Logic
    prompt = f"""### INSTRUCTION
Extract historical data from the Notes and Ship_Notes columns. Return ONLY values separated by '|'. 
Use '-' for missing info. Your output must contain exactly 32 values.

### BATCH PROCESSING RULE
Multiple records often share the same 'Ship_Notes'. The following fields MUST remain consistent for shared ships:
- Ship_Name, Commander, Arrival_Port, Arrival_Port_Country, Arrival_Coordinates.

### FIELD ORDER (32 COLUMNS)
ID | Book | First_Name | Surname | Ship_Name | Notes | Ship_Notes | Birthdate | Gender | Race | Ethnicity | Origin | Extracted_City | Extracted_County | Extracted_State | Extracted_Area | Country | Departure_Coordinates | Departure_Port | Departure_Date | Arrival_Port | Arrival_Port_Country | Arrival_Coordinates | Father_FirstName | Father_Surname | Mother_FirstName | Mother_Surname | Ref_Page | Commander | Enslaver | Primary_Source_1 | Primary_Source_2

### IDENTITY & EXTRACTION RULES
1. **Race, Ethnicity, & Origin Logic**:
   - If 'Mulatto' is mentioned: Race -> Mulatto, Ethnicity -> Mixed Race.
   - If 'Quadroon' is mentioned: Race -> Quadroon, Ethnicity -> Mixed Race.
   - If heritage is mixed (e.g., 'Indian & Span' or 'mother an Indian'): Ethnicity -> Mixed Race, Origin -> [Specific Heritage].
2. **Birthdate**: Extract ONLY the age as a number.
3. **Gender**: Use 'Child Male/Female' if age < 18, otherwise 'Male/Female'.
4. **Geography**: 'Extracted_State' must be a valid US State; 'Country' must be a valid Country.
5. **Don't change**: Return the existing DataFrame value exactly as-is; do not modify using Notes or Ship_Notes.

### EXAMPLES
- Notes: 'Charles Allen, 25, stout, M, between an Indian & Span., (Pioneer, KAD). Free by General Birch's certificate says he lived with Matthew Hobbs of Sussex County, Maryland, until 25.'
  Ship_Notes: 'Ship Lady's Adventure bound for St. John's Capt. Robt. Gibson' 
  Output: (Don't change) | (Don't change) | Charles | Allen | (Don't change) | (Don't change) | (Don't change) | 25 | Male | Mulatto | Mixed Race | Indian / Spanish | - | Sussex | Maryland | - | United States | [Coords] | [Port] | 1783 | [Port] | [Country] | [Coords] | - | - | - | - | - | [Commander] | Pioneer, KAD | [Source1] | [Source2]

- Notes: 'Sarah Johnson, 22, squat wench, quadroon, (Donald Ross). Formerly slave to Burgess Smith, Lancaster County; left him with the above Thomas Johnson her husband. GBC'
  Ship_Notes: 'Tree Briton bound for Port Roseway Jacob Hays, Master' 
  Output: (Don't change) | (Don't change) | Sarah | Johnson | (Don't change) | (Don't change) | (Don't change) | 22 | Female | Quadroon | Mixed Race | - | - | Lancaster | Pennsylvania | - | United States | [Coords] | [Port] | 1783 | [Port] | [Country] | [Coords] | - | - | - | - | - | [Commander] | Donald Ross |

TEXT TO PROCESS:
{context}

RESPONSE FORMAT (EXACTLY 32 VALUES SEPARATED BY '|'):
"""
    
    try:
        # Rate limit compliance: 60 RPM = 1.1s delay between requests
        time.sleep(1.1) 
        
        completion = client.chat.completions.create(
            model="qwen/qwen3-32b",
            messages=[{"role": "user", "content": prompt}],
            temperature=0, # Keeping it 0 for extraction accuracy despite sample 0.6
            max_completion_tokens=4096,
            top_p=0.95,
            stream=False, # Set to False for cleaner parsing in batch scripts
            extra_headers={"X-Groq-Prompt-Caching": "on"}
        )
        
        raw_output = completion.choices[0].message.content.strip()
        extracted_line = next((line.strip() for line in raw_output.split('\n') if '|' in line), raw_output)
        clean_output = re.sub(r'(?i)[a-z_\s]+:\s*', '', extracted_line).strip()
        parts = [p.strip() for p in clean_output.split('|')]
        
        while len(parts) < 32:
            parts.append("-")
        return parts[:32]
        
    except Exception as e:
        print(f"Error processing row: {e}")
        return ["-"] * 32

# --- 2. Main Execution ---
def main():
    input_file = 'Consolidated_Directory_v12_subset.xlsx' 
    output_file = 'Validated_Records_Fixed_Qwen.xlsx'
    
    df = pd.read_excel(input_file)
    
    # Check against Daily Limit
    if len(df) > 1000:
        print(f"⚠️ Warning: Dataset size ({len(df)}) exceeds Qwen daily limit (1,000 requests/day).")
        print("Processing the first 1,000 records only.")
        df = df.head(1000)

    print(f"Validating {len(df)} records using Qwen 3-32B...")
    start_time = time.time()
    
    # Use 1 worker to strictly respect the 60 RPM / 6K TPM limits
    with ThreadPoolExecutor(max_workers=1) as executor:
        results = list(tqdm(
            executor.map(ollama_validate_and_fix, [row for _, row in df.iterrows()]), 
            total=len(df), 
            desc="Auditing Records"
        ))
    
    validated_cols = [
        'ID', 'Book', 'First_Name', 'Surname', 'Ship_Name', 'Notes', 'Ship_Notes',
        'Birthdate', 'Gender', 'Race', 'Ethnicity', 'Origin', 'Extracted_City', 
        'Extracted_County', 'Extracted_State', 'Extracted_Area', 'Country', 
        'Departure_Coordinates', 'Departure_Port', 'Departure_Date', 'Arrival_Port', 
        'Arrival_Port_Country', 'Arrival_Coordinates', 'Father_FirstName', 
        'Father_Surname', 'Mother_FirstName', 'Mother_Surname', 'Ref_Page', 
        'Commander', 'Enslaver', 'Primary_Source_1', 'Primary_Source_2'
    ]
    
    for i, col in enumerate(validated_cols):
        df[col] = [result[i] if i < len(result) else "-" for result in results]
    
    def calc_birth(extracted_age):
        try:
            match = re.search(r'\d+', str(extracted_age))
            if match:
                return 1783 - int(match.group())
            return "-"
        except:
            return "-"
    
    print("Calculating birth years...")
    df['Birthdate'] = df['Birthdate'].apply(calc_birth)
    
    df.to_excel(output_file, index=False)
    
    elapsed_time = round(time.time() - start_time, 2)
    print(f"✨ SUCCESS. File saved: {output_file}")
    print(f"Qwen Daily Quota Used: {len(df)}/1,000")

if __name__ == "__main__":
    main()