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
client = Groq(api_key=os.getenv("GROQ_API_KEY"))

def extract_ship_data(ship_notes):
    """Extract consistent ship data from Ship_Notes string."""
    commander = '-'
    arrival_port = '-'
    
    # Extract commander (Capt./Master + Name)
    commander_match = re.search(r'(?:Capt\.?|Master|Captain)\s+([A-Za-z\.\s]+?)(?:\s+(?:bound|$))', ship_notes)
    if commander_match:
        commander = commander_match.group(1).strip()
    
    # Extract arrival port (after "bound for")
    port_match = re.search(r'bound for\s+([^C]*?)(?:\s+Capt\.?|$)', ship_notes)
    if port_match:
        arrival_port = port_match.group(1).strip()
    
    return {
        'commander': commander,
        'arrival_port': arrival_port
    }

def ollama_validate_and_fix(row):
    """
    Validates and enriches historical record data using Groq LLM.
    Extracts age, locations, family relations, and identity information.
    Returns: [Age, Gender, Race, Ethnicity, Origin, City, County, State, Father_Names, Mother_Names, Enslaver]
    """
    current_data = row.to_dict()
    notes = str(current_data.get('Notes', ''))
    ship_notes = str(current_data.get('Ship_Notes', ''))
    
    ship_info = extract_ship_data(ship_notes)
    
    # Build a clear context for the LLM
    context = f"""RECORD TO VALIDATE:
Person: {current_data.get('First_Name', '-')} {current_data.get('Surname', '-')}
Current Gender: {current_data.get('Gender', '-')}
Current Race: {current_data.get('Race', '-')}
Current Ethnicity: {current_data.get('Ethnicity', '-')}

Biographical Notes: {notes}
Ship Information: {ship_notes}
"""
    
    prompt = f"""### TASK
You are validating a historical record from the Book of Negroes (1783 evacuation). 
Extract and clarify key information from the biographical notes.

Return EXACTLY 11 values separated by '|' (use '-' for unknown).

### EXTRACTION GUIDELINES

1. **AGE**: Extract numeric age only (e.g., "35" from "age 35" or "35 years old")
   - If age < 12, flag as child
   - If age not mentioned, return '-'

2. **GENDER**: Return 'Male', 'Female', 'Child Male', or 'Child Female'
   - Check for words like "man", "woman", "boy", "girl", "wench" (historical term)
   - Update only if Notes clearly specifies a different gender

3. **RACE & ETHNICITY**: Based on explicit mentions in Notes
   - Mulatto → Race: Mulatto, Ethnicity: Mixed Race
   - Quadroon → Race: Quadroon, Ethnicity: Mixed Race
   - "Black" or "African" → Race: Black, Ethnicity: African American
   - If no descriptor or "Black" and no mixed heritage mention: keep current values
   - If mixed heritage mentioned (Indian, Spanish, etc.): Ethnicity: Mixed Race

4. **ORIGIN**: Specific heritage if mixed (e.g., "Indian/Spanish", "Indian", "Portuguese")
   - Extract from phrases like "between Indian & Spanish" or ancestors
   - Return '-' if no mixed heritage

5. **EXTRACTED LOCATION** (City, County, State):
   - Look for place names after: "lived with", "of", "from", "nigh", "near"
   - **County**: Usually marked as "[Name] County" 
   - **State/Province**: Valid colonial/US states (Maryland, Virginia, Pennsylvania, New York, Carolina, Nova Scotia, etc.)
   - **City**: Specific towns (Philadelphia, New York, London, etc.)
   
   Example: "lived with Mr. Moore of Reedy Island, Caroline" → City: Reedy Island, State: Carolina

6. **FAMILY RELATIONS**: Look for explicit mentions
   - Father: "son of [Name]", "father [Name]"
   - Mother: "mother [Name]", "daughter of [Name]"
   - Return format: "FirstName Surname" or "-"

7. **ENSLAVER/OWNER**: Usually in parentheses or after "property of"
   - Examples: "(Richard Browne)", "property of Thomas Richard"
   - Extract the name

### OUTPUT FORMAT (11 VALUES PIPE-SEPARATED):
Age | Gender | Race | Ethnicity | Origin | Extracted_City | Extracted_County | Extracted_State | Father_FirstName_Surname | Mother_FirstName_Surname | Enslaver_Name

### EXAMPLES

Example 1:
Input: "Billy Williams, 35, healthy stout man, (Richard Browne). Formerly lived with Mr. Moore of Reedy Island, Caroline, from whence he came with the 71st Regiment about 3 years ago."
Output: 35 | Male | Black | African American | - | Reedy Island | - | Carolina | - | - | Richard Browne

Example 2:
Input: "Sarah Johnson, 22, squat wench, quadroon, (Donald Ross). Formerly slave to Burgess Smith, Lancaster County; left with Thomas Johnson, her husband. GBC"
Output: 22 | Female | Quadroon | Mixed Race | - | - | Lancaster | Pennsylvania | - | - | Burgess Smith

Example 3:
Input: "Charles Allen, 25, stout, between an Indian & Spanish. (Pioneer). Free by General Birch's certificate. Lived with Matthew Hobbs of Sussex County, Maryland."
Output: 25 | Male | Mulatto | Mixed Race | Indian/Spanish | - | Sussex | Maryland | - | - | Pioneer

---

{context}

RESPOND WITH EXACTLY 11 PIPE-SEPARATED VALUES AND NOTHING ELSE:
"""
    
    try:
        # Respect 30 RPM limit (30 requests/minute ≈ 2.1s per request)
        time.sleep(2.1) 
        
        completion = client.chat.completions.create(
            model="llama-3.1-8b-instant",
            messages=[{"role": "user", "content": prompt}],
            temperature=0, 
            max_completion_tokens=200,
            stop=["\n", "---", "END"]
        )
        
        raw_output = completion.choices[0].message.content.strip()
        
        # Extract the line with pipes
        extracted_line = next((line.strip() for line in raw_output.split('\n') if '|' in line), raw_output)
        
        # Clean up any labels that might be prepended
        extracted_line = re.sub(r'^[^|]*?:\s*', '', extracted_line).strip()
        
        # Split by pipe and clean each value
        parts = [p.strip() for p in extracted_line.split('|')]
        
        # Ensure exactly 11 parts
        while len(parts) < 11:
            parts.append("-")
        
        return parts[:11]
        
    except Exception as e:
        print(f"Error processing row {current_data.get('ID', 'unknown')}: {e}")
        return ["-"] * 11

def apply_validated_data(df, validated_results):
    """
    Apply validated data from LLM back to dataframe.
    Only update cells where LLM found new/better information.
    """
    output_cols = [
        'Birthdate', 'Gender', 'Race', 'Ethnicity', 'Origin', 
        'Extracted_City', 'Extracted_County', 'Extracted_State', 
        'Father_FullName', 'Mother_FullName', 'Enslaver'
    ]
    
    for i, idx in enumerate(df.index):
        if i < len(validated_results):
            result = validated_results[i]
            for j, col in enumerate(output_cols):
                if j < len(result):
                    value = result[j]
                    if value != '-':  # Only update if LLM found something
                        df.loc[idx, col] = value
    
    return df

def calculate_birthyear(age_str, birth_year=1783):
    """Convert age to birth year."""
    try:
        match = re.search(r'\d+', str(age_str))
        if match:
            age = int(match.group())
            return birth_year - age
        return "-"
    except:
        return "-"

# --- 2. Main Execution ---
def main():
    input_file = 'Consolidated_Directory_v12_subset.xlsx' 
    output_file = 'Validated_Records_Cleaned.xlsx'
    
    df = pd.read_excel(input_file)
    print(f"Validating {len(df)} records...")
    
    # Initialize new columns if they don't exist
    new_cols = ['Father_FullName', 'Mother_FullName', 'Enslaver']
    for col in new_cols:
        if col not in df.columns:
            df[col] = '-'
    
    start_time = time.time()
    
    # Process records with single worker to respect rate limits
    with ThreadPoolExecutor(max_workers=1) as executor:
        results = list(tqdm(
            executor.map(ollama_validate_and_fix, [row for _, row in df.iterrows()]), 
            total=len(df), 
            desc="Validating Records"
        ))
    
    # Apply results to dataframe
    output_cols = [
        'Birthdate', 'Gender', 'Race', 'Ethnicity', 'Origin', 
        'Extracted_City', 'Extracted_County', 'Extracted_State', 
        'Father_FullName', 'Mother_FullName', 'Enslaver'
    ]
    
    for i, col in enumerate(output_cols):
        df[col] = [result[i] if i < len(result) else "-" for result in results]
    
    print("Calculating birth years from ages...")
    df['Birthyear'] = df['Birthdate'].apply(lambda x: calculate_birthyear(x))
    
    # Save results
    df.to_excel(output_file, index=False)
    elapsed = round(time.time() - start_time, 2)
    print(f"✨ SUCCESS. Saved to {output_file}. Processed in {elapsed}s")
    print(f"\nSample results (first 3 rows):")
    print(df[['First_Name', 'Surname', 'Age', 'Gender', 'Race', 'Extracted_State', 'Enslaver']].head(3).to_string())

if __name__ == "__main__":
    main()
