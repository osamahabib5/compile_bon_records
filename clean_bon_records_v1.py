import pandas as pd
import re
import time
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
from ollama import Client

# --- 1. Initialization ---
client = Client(host='http://localhost:11434')

def ollama_validate_and_fix(row):
    """
    Strict Auditor: Compares row data to Notes and returns ONLY values.
    Uses optimized prompt with detailed instructions for accurate extraction.
    """
    current_data = row.to_dict()
    
    # Prepare context with all available information
    context = (
        f"Notes: {current_data.get('Notes', '-')}\n"
        f"Ship_Notes: {current_data.get('Ship_Notes', '-')}"
    )
    
    # OPTIMIZED PROMPT with detailed instructions
    prompt = f"""### INSTRUCTION
Extract historical data. Return ONLY values separated by '|'. Use '-' for missing info.

### FIELD ORDER
First_Name | Surname | Ship_Name | Arrival_Port | Arrival_Country | Arrival_Coordinates | Extracted_City | Extracted_County | Extracted_State | Extracted_Area | Country | Departure_Coordinates | Commander | Age

### LOGIC & EXAMPLES
1. DEPARTURE LOGIC:
   - Notes: 'Billy Williams, 35, healthy stout man, (Richard Browne). Formerly lived with Mr. Moore of Reedy Island, Caroline, from whence he came with the 71st Regiment about 3 years ago.' -> 
     Ship_Notes: 'Ship Aurora bound for St. John's'
    
     First_Name: Billy, Surname: Williams, Ship_Name: Aurora, Arrival_Port: St. John's, 
     Arrival_Country: Canada, Arrival_Coordinates: 47.5704, -52.7129
     Extracted_City: -, Extracted_County: -, Extracted_Area: Reedy Island (since it's not 
     a city or County), Extracted_State: Delaware (inferred based on Reedy Island), Country: United States (inferred based
     on State and Area name), Departure_Coordinates: 39.7392, -75.5398, Commander: -, Age: 35

   - Notes: 'Rose Richard, 20, healthy young woman, (Thomas Richard). Property of Thomas Richard, a refugee from Philadelphia.' -> 
     Ship_Notes: 'Ship Aurora bound for St. John's'
    
     First_Name: Rose, Surname: Richard, Ship_Name: Aurora, Arrival_Port: St. John's, 
     Arrival_Country: Canada, Arrival_Coordinates: 47.5704, -52.7129
     Extracted_City: -, Extracted_County: -, Extracted_Area: -, Extracted_State: Pennsylvania, Country: United States (inferred based
     on State and Area name), Departure_Coordinates: 39.9526, -75.1652, Commander: -, Age: 20

   - 'John Chapman of Princess Ann County, Virginia' -> Extracted_County: Princess Ann, Extracted_State: Virginia, Country: United States
   - 'St. Paul's, London' -> Extracted_City: London, Country: United Kingdom
   - 'Kingston, Jamaica' -> Extracted_City: Kingston, Country: Jamaica
   - 'Head of Elk' -> Extracted_City: Elkton, Extracted_State: Maryland, Country: United States, Departure_Coordinates: 39.6068, -75.8333

2. EXTRACTION RULES:
   - Extracted_State should have only a correct US state name. If you can't be sure, put '-'
   - Country should have only a correct country name. If you can't be sure, put '-'
   - Coordinates should be in format 'latitude, longitude' with only numbers. If you can't find them, put '-'
   - Age is typically given just after the name in Notes. Extract as a number. If not found, put '-'
   - Commander name can ONLY come from Ship_Notes and must be a Name string. Don't infer it. If not found, put '-'
   - Ship_Name comes from Ship_Notes only
   - Arrival_Port and Arrival_Country come from Ship_Notes only
   - Use '-' for any field you cannot confidently extract

TEXT TO PROCESS:
{context}

RESPONSE FORMAT (EXACTLY 14 VALUES SEPARATED BY '|'):
"""
    
    try:
        response = client.generate(
            model='qwen2.5:7b',
            prompt=prompt,
            options={
                "num_ctx": 2048,  # Increased for more context
                "temperature": 0,
                "num_predict": 200,  # Slightly increased for detailed responses
                "stop": ["TEXT TO", "INSTRUCTION", "FIELD ORDER", "LOGIC", "RULES", "\n\n"]
            }
        )
        
        raw_output = response['response'].strip()
        
        # Post-Processing: Remove any extra text before the actual values
        # Extract only the pipe-separated values
        lines = raw_output.split('\n')
        
        # Find the line with actual data (contains pipes and values)
        extracted_line = None
        for line in lines:
            if '|' in line:
                extracted_line = line.strip()
                break
        
        if not extracted_line:
            extracted_line = raw_output
        
        # Remove any remaining labels or extra text
        clean_output = re.sub(r'(?i)[a-z_\s]+:\s*', '', extracted_line)
        clean_output = clean_output.strip()
        
        # Split by pipe and clean each part
        parts = [p.strip() for p in clean_output.split('|')]
        
        # Ensure exactly 14 columns
        while len(parts) < 14:
            parts.append("-")
        
        parts = parts[:14]
        
        return parts
        
    except Exception as e:
        print(f"Error processing row: {e}")
        return ["-"] * 14


# --- 2. Main Execution ---
def main():
    input_file = 'Consolidated_Directory_v12_subset.xlsx' 
    output_file = 'Consolidated_Directory_v12_ollama.xlsx'
    
    df = pd.read_excel(input_file)
    print(f"Validating {len(df)} records. Applying Optimized Extraction Protocol...")
    print(f"Total records to process: {len(df)}\n")
    
    start_time = time.time()
    
    # Process with 2 workers for i5 stability
    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(tqdm(
            executor.map(ollama_validate_and_fix, [row for _, row in df.iterrows()]), 
            total=len(df), 
            desc="Auditing Records",
            unit="record"
        ))
    
    # Define validated columns in the correct order (must match prompt field order)
    validated_cols = [
        'First_Name', 
        'Surname', 
        'Ship_Name', 
        'Arrival_Port', 
        'Arrival_Country', 
        'Arrival_Coordinates',
        'Extracted_City', 
        'Extracted_County', 
        'Extracted_State', 
        'Extracted_Area', 
        'Country', 
        'Departure_Coordinates',
        'Commander', 
        'Age'
    ]
    
    # Assign results directly to DataFrame columns
    print("\nAssigning validated data to DataFrame...")
    for i, col in enumerate(validated_cols):
        df[col] = [result[i] if i < len(result) else "-" for result in results]
    
    # Verify the data was assigned correctly
    print("\n" + "="*80)
    print("VALIDATION PREVIEW - First 5 rows of extracted data:")
    print("="*80)
    print(df[validated_cols].head().to_string())
    print("="*80 + "\n")
    
    # Calculate Birthdate in Python (1783 - Age)
    def calc_birth(age):
        try:
            match = re.search(r'\d+', str(age))
            if match:
                return 1783 - int(match.group())
            return "-"
        except:
            return "-"
    
    df['Birthdate'] = df['Age'].apply(calc_birth)
    
    # Write to Excel
    df.to_excel(output_file, index=False)
    
    elapsed_time = round(time.time() - start_time, 2)
    print(f"✨ SUCCESS")
    print(f"Total Time: {elapsed_time}s")
    print(f"Records Processed: {len(df)}")
    print(f"Output File: {output_file}")
    print(f"Processing Rate: {round(len(df)/elapsed_time, 2)} records/second")

if __name__ == "__main__":
    main()