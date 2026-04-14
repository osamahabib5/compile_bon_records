import pandas as pd
from ollama import Client
import time
import re
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm

# 1. Initialize Client
client = Client(host='http://localhost:11434')

def clean_data_genealogy(row):
    notes = str(row.get('Notes', ""))
    ship_notes = str(row.get('Ship_Notes', ""))
    context = f"Notes: {notes} | Ship_Notes: {ship_notes}"
    
    # DETAILED PROMPT INTEGRATING ALL YOUR EXAMPLES
    prompt = f"""
    ### INSTRUCTION
    Extract historical data. Return ONLY values separated by '|'. Use '-' for missing info.
    
    ### FIELD ORDER
    First_Name | Surname | Ship_Name | Arrival_Port| Arrival_Country | Arrival_Coordinates | Extracted_City | Extracted_County
      | Extracted_State | Extracted_Area | Country | Departure_Coordinates | Commander | Age

    ### LOGIC & EXAMPLES
    1. DEPARTURE LOGIC:
       - Notes: 'Billy Williams, 35, healthy stout man, (Richard Browne). Formerly lived with Mr. Moore of Reedy Island, Caroline, from whence he came with the 71st Regiment about 3 years ago.' -> 
         Ship_Notes: 'Ship Aurora bound for St. John's'
        
         First_Name: Billy, Surname: Williams, Ship_Name: Aurora, Arrival_Port: St. John's, 
         Arrival_Country: Canada, Arrival_Coordinates: (Get numeric coordinates for St. John's, Canada)
         Extracted_City: -, Extracted_County: -, Extracted_Area: Reedy Island (since it's not 
         a city or County), Extracted_State: Delaware (inferred based on Reedy Island), Country: United States (inferred based
         on State and Area name), Departure_Coordinates: '39.7392, -75.5398', Commander: - (Commander name could
         only come from Ship Notes. Don't infer it), Age: 35.

         Notes: 'Rose Richard, 20, healthy young woman, (Thomas Richard). Property of Thomas Richard, a refugee from Philadelphia.' -> 
         Ship_Notes: 'Ship Aurora bound for St. John's'
        
         First_Name: Rose, Surname: Richard, Ship_Name: Aurora, Arrival_Port: St. John's, 
         Arrival_Country: Canada, Arrival_Coordinates: (Get numeric coordinates for St. John's, Canada)
         Extracted_City: -, Extracted_County: -, Extracted_Area: -, Extracted_State: Philadelphia , Country: United States (inferred based
         on State and Area name), Departure_Coordinates: (Get numeric coordinates for Philadelphia), Commander: - (Commander name could
         only come from Ship Notes. Don't infer it), Age: 20.

       - 'John Chapman of Princess Ann County, Virginia' -> Extracted_County: Princess Ann, Extracted_State: Virginia, Country: United States.
       - 'St. Paul's, London' -> Extracted_City: London, Country: United Kingdom.
       - 'Kingston, Jamaica' -> Extracted_City: Kingston, Country: Jamaica.
       - 'Head of Elk' -> Extracted_City: Elkton, Extracted_State: Maryland, Country: United States, Departure_Coordinates: '39.6068, -75.8333'.
       - Make sure the Extracted_State column should have only a correct US state name. Similarly, Country should have only a correct country name. If you can't be sure, put '-'.
       - Coordinates should be in the format 'latitude, longitude' and only contain numbers. If you can't find coordinates, put '-'.
       - Age is given in almost all of the Notes. Extract it as a number. If you can't find it, put '-'.It would come just after the name.
       - Commander name can only be extracted from Ship_Notes and can only be a Name and String. If you can't find it, put '-'.
    

    TEXT TO PROCESS:
    {context}
    """

    try:
        response = client.generate(
            model='qwen2.5:7b',
            prompt=prompt,
            options={
                "num_ctx": 1536, 
                "temperature": 0,
                "num_predict": 250
            }
        )
        
        # Clean response and ensure no labels are present
        raw_output = response['response'].strip().split('\n')[0]
        raw_output = re.sub(r'^[A-Za-z_ ]+: ', '', raw_output)
        
        parts = [p.strip() for p in raw_output.split('|')]
        while len(parts) < 14:
            parts.append("-")
        return parts[:14]
            
    except Exception:
        return ["-"] * 14

# --- Main Execution ---
input_file = 'Consolidated_Directory_v12_subset.xlsx'
output_file = 'Corrected_Genealogy_Final.xlsx'

df = pd.read_excel(input_file)
MAX_WORKERS = 2  # Best for stability on i5

print(f"Starting extraction for {len(df)} records...")
start_time = time.time()

# 2. RUN IN PARALLEL
with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    results = list(tqdm(executor.map(clean_data_genealogy, [row for _, row in df.iterrows()]), 
                        total=len(df), 
                        desc="Processing Rows"))

# 3. Create DataFrame from Results
new_cols = [
    'First_Name', 'Surname', 'Ship_Name', 'Arrival_Port_City', 'Arrival_Country', 'Arrival_Coordinates',
    'Extracted_City', 'Extracted_County', 'Extracted_State', 'Extracted_Area', 'Country', 'Departure_Coordinates',
    'Commander', 'Age'
]
df[new_cols] = pd.DataFrame(results, index=df.index)

# 4. PYTHON-BASED BIRTHDATE CALCULATION
def get_birthdate(age_str):
    try:
        match = re.search(r'\d+', str(age_str))
        if match:
            return 1783 - int(match.group())
    except:
        pass
    return "-"

df['Birthdate'] = df['Age'].apply(get_birthdate)

# 5. Save Output
df.to_excel(output_file, index=False)
total_time = round(time.time() - start_time, 2)

print(f"\nCompleted in {total_time}s. Average: {round(total_time/len(df), 2)}s per row.")