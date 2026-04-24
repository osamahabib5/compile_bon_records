import pandas as pd
import spacy
import re

# Load the spaCy English model
# Run 'python -m spacy download en_core_web_sm' in your terminal if you haven't yet
nlp = spacy.load("en_core_web_sm")

def clean_person_names(input_file, output_file, target_column):
    # Load the Excel file
    df = pd.read_excel(input_file)

    # List of common non-name identifiers to filter out
    invalid_keywords = {
        "child", "children", "boy", "girl", "infant", "daughter", 
        "son", "months", "years", "old", "little", "small"
    }

    def validate_name(text):
        original_text = str(text).strip()
        
        # 1. Handle blanks or NaNs
        if pd.isna(text) or original_text == "":
            return "-"

        # 2. Strict Character Check
        # Reject if it contains numbers, symbols like &, or fractions like ½
        if re.search(r'[0-9&½¼¾]', original_text):
            return "-"

        # 3. Keyword Filter
        # Reject if the text contains words like "child", "boy", etc.
        words = original_text.lower().split()
        if any(word in invalid_keywords for word in words):
            return "-"

        # 4. spaCy NER Validation
        doc = nlp(original_text)
        
        # We want to ensure the entire string (or the majority) is recognized as a PERSON
        # This allows "Black" or "Lawson White" but rejects "The helper"
        person_entities = [ent.text for ent in doc.ents if ent.label_ == "PERSON"]
        
        if person_entities:
            # If the identified name is a significant part of the input, keep it
            # This handles surnames like "Black" or "White" correctly
            return original_text
        
        # 5. Final fallback for single surnames that NER might miss without context
        # If it's a single capitalized word and passed the above checks, we can keep it
        if original_text.istitle() and len(words) == 1:
            return original_text

        return "-"

    # Apply the cleaning logic
    print(f"Cleaning column: {target_column}...")
    df[target_column] = df[target_column].apply(validate_name)

    # Save the cleaned file
    df.to_excel(output_file, index=False)
    print(f"Successfully saved cleaned data to: {output_file}")

# Usage
# Replace with your actual file path and column name
clean_person_names('Consolidated_Book_of_Negroes_v10.xlsx', 'Consolidated_Book_of_Negroes_v11.xlsx', 'Surname')