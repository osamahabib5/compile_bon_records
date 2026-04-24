import pandas as pd
import re

def clean_excel_columns(input_file, output_file):
    # Load the Excel file
    df = pd.read_excel(input_file)
    
    # Define a function to clean strings
    def clean_name(name):
        # Replace spaces, slashes, or backslashes with underscores
        # The regex [ / \\] matches space, forward slash, or backslash
        cleaned = re.sub(r'[ / \\]+', '_', str(name))
        # Optional: Remove leading/trailing underscores and double underscores
        return cleaned.strip('_').replace('__', '_')

    # Rename the columns
    df.columns = [clean_name(col) for col in df.columns]
    
    # Save to a new Excel file
    df.to_excel(output_file, index=False)
    print(f"Cleaned file saved as: {output_file}")

# Usage
clean_excel_columns('USCTs_Connecticut_rev_02_copy.xlsx', 'USCTs_Connecticut_rev_02_copy.xlsx')