import pandas as pd
import re

def clean_excel_columns(input_file, output_file):
    # Load the Excel file
    excel_file = pd.ExcelFile(input_file)
    
    # Define a function to clean strings
    def clean_name(name):
        # Replace spaces, slashes, or backslashes with underscores
        # The regex [ / \\] matches space, forward slash, or backslash
        cleaned = re.sub(r'[ / \\]+', '_', str(name))
        # Optional: Remove leading/trailing underscores and double underscores
        return cleaned.strip('_').replace('__', '_')

    # Process all sheets
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        for sheet_name in excel_file.sheet_names:
            df = pd.read_excel(input_file, sheet_name=sheet_name)
            # Rename the columns
            df.columns = [clean_name(col) for col in df.columns]
            # Save to Excel file
            df.to_excel(writer, sheet_name=sheet_name, index=False)
    print(f"Cleaned file saved as: {output_file}")

# Usage
clean_excel_columns('Database_template_records_insertion_JO_Attaquin_copy.xlsx', 'Database_template_records_insertion_JO_Attaquin_v1.xlsx')