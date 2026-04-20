import pandas as pd
import spacy
import re

# --- 1. SETUP ---
# Load the medium model for better entity recognition accuracy
try:
    nlp = spacy.load("en_core_web_md")
except OSError:
    from spacy.cli import download
    download("en_core_web_md")
    nlp = spacy.load("en_core_web_md")

INPUT_FILE = 'Consolidated_Book_of_Negroes_v8.xlsx'
OUTPUT_FILE = 'Consolidated_Book_of_Negroes_v9.xlsx'

# --- 2. LOGIC FUNCTIONS ---

def extract_commander_with_spacy(row):
    """
    STAGE 1: NLP-based extraction.
    Uses spaCy to identify only PERSON entities, ignoring 'bound for' segments.
    """
    notes = str(row.get('Ship_Notes', "")).strip()
    
    if not notes or notes in ["-", "nan"]:
        return str(row.get('Commander', "-"))

    # Remove 'bound for' and everything before it to isolate the destination/commander segment
    # This ensures 'bound for' itself never ends up in the column.
    if "bound for" in notes.lower():
        parts = re.split(r'bound for', notes, flags=re.IGNORECASE)
        candidate_text = parts[1].strip() if len(parts) > 1 else ""
    else:
        candidate_text = notes

    # Process the text with spaCy
    doc = nlp(candidate_text)
    
    # Filter strictly for entities labeled as PERSON (Proper Names)
    # We take the last one found because the name usually follows the port description
    person_entities = [ent.text for ent in doc.ents if ent.label_ == "PERSON"]
    
    if person_entities:
        return person_entities[-1].strip()

    # FALLBACK: If spaCy misses the entity, look for 'Name, Master'
    pattern_master = re.search(r"([A-Z][\w\s.'-]+),\s*(?:Master|Capt|Lt|Captain|Commander)", candidate_text, re.IGNORECASE)
    if pattern_master:
        return pattern_master.group(1).strip()

    # FINAL HEURISTIC: Last two capitalized words (only if no persona found)
    words = [w.strip(".,; ") for w in candidate_text.split()]
    potential = [w for w in words if w and w[0].isupper() and len(w) > 1]
    if potential and len(potential) >= 2:
        return " ".join(potential[-2:])
        
    return "-"

def validate_commander_final(row):
    """
    STAGE 2: Strict Validation Pass.
    Ensures Commander != Ship_Name and Commander != Arrival_Port.
    """
    commander = str(row.get('Commander', "-")).strip()
    arrival_port = str(row.get('Arrival_Port', "-")).strip()
    ship_name = str(row.get('Ship_Name', "-")).strip()

    if commander == "-" or not commander:
        return "-"

    # Normalize for comparison
    cmd_lower = commander.lower()
    port_lower = arrival_port.lower()
    ship_lower = ship_name.lower()

    # 1. Identity Check
    if (port_lower != "-" and cmd_lower == port_lower) or \
       (ship_lower != "-" and cmd_lower == ship_lower):
        return "-"

    # 2. Substring Scrubbing
    # Remove arrival port if it was mistakenly included in the name string
    if port_lower != "-" and port_lower in cmd_lower:
        commander = re.sub(re.escape(arrival_port), "", commander, flags=re.IGNORECASE).strip()
    
    # Remove ship name if it was mistakenly included
    if ship_lower != "-" and ship_lower in cmd_lower:
        commander = re.sub(re.escape(ship_name), "", commander, flags=re.IGNORECASE).strip()

    # Final cleanup of artifacts (commas, dots, extra spaces)
    commander = re.sub(r'^[^\w]+|[^\w]+$', '', commander).strip()
    
    return commander if len(commander) > 1 else "-"

def clean_enslaver(row):
    """
    STAGE 3: Clean Enslaver Names by stripping [] and ().
    """
    notes = str(row.get('Notes', ""))
    match = re.search(r'[\(\[](.*?)[\)\]]', notes)
    if match:
        content = match.group(1).strip()
        content = re.sub(r'[\[\]\(\)]', '', content).strip()
        if "own bottom" in content.lower():
            return "-"
        return content
    return str(row.get('Enslaver', "-"))

# --- 3. MAIN PROCESS ---

def main():
    print(f"Loading {INPUT_FILE}...")
    df = pd.read_excel(INPUT_FILE)

    # Note: Ensure validation columns exist
    for col in ['Arrival_Port', 'Ship_Name']:
        if col not in df.columns:
            df[col] = "-"

    print("Step 1: Extracting proper names using spaCy PERSON entities...")
    df['Commander'] = df.apply(extract_commander_with_spacy, axis=1)

    print("Step 2: Cleaning and validating against Ship and Port names...")
    df['Commander'] = df.apply(validate_commander_final, axis=1)

    print("Step 3: Cleaning Enslaver bracketed entries...")
    df['Enslaver'] = df.apply(clean_enslaver, axis=1)

    # Set up the strict 33-column schema
    column_order = [
        'ID', 'Book', 'First_Name', 'Surname', 'Ship_Name', 'Notes', 'Ship_Notes',
        'Birthdate', 'Gender', 'Race', 'Ethnicity', 'Origin', 'City', 'County', 'State', 'Landmark', 'Country',
        'Areas_for_coordinates', 'Final_Coordinates', 'Departure_Port', 'Departure_Date', 'Arrival_Port',
        'Arrival_Port_Country', 'Arrival_Coordinates', 'Father_FirstName', 'Father_Surname', 'Mother_FirstName',
        'Mother_Surname', 'Ref_Page', 'Commander', 'Enslaver', 'Primary_Source_1', 'Primary_Source_2'
    ]

    # Fill missing columns and reorder
    for col in column_order:
        if col not in df.columns:
            df[col] = "-"

    df_final = df[column_order]

    print(f"Exporting to {OUTPUT_FILE}...")
    df_final.to_excel(OUTPUT_FILE, index=False)
    print("Process Complete.")

if __name__ == "__main__":
    main()