import pandas as pd
import re

def clean_value(val):
    """
    Removes leading/trailing whitespace and irregular characters 
    (punctuation, brackets, etc.) from the start and end of a string.
    """
    if not isinstance(val, str):
        if pd.isna(val): return ""
        val = str(val)
    
    # Remove leading/trailing whitespace
    val = val.strip()
    # Remove leading/trailing non-alphanumeric characters (except spaces)
    # This targets things like: "[John]", "Ship!", "'Name'"
    val = re.sub(r'^[^a-zA-Z0-9]+|[^a-zA-Z0-9]+$', '', val)
    return val.strip()

def find_missing_records(file_a_path, file_b_path, output_path):
    # 1. Load the Excel files
    df_a = pd.read_excel(file_a_path)
    df_b = pd.read_excel(file_b_path)

    # 2. Pre-process and Clean Data
    # Apply cleaning to the join and comparison columns
    for col in ['Ship_Name', 'Name']:
        if col in df_a.columns:
            df_a[col] = df_a[col].apply(clean_value)
            
    for col in ['Ship_Name', 'First_Name', 'Surname']:
        if col in df_b.columns:
            df_b[col] = df_b[col].apply(clean_value)

    # 3. Create a temporary ID for tracking File A records
    df_a['temp_id'] = range(len(df_a))

    # 4. Merge FileA with FileB on Ship_Name
    merged_with_id = pd.merge(df_a, df_b, on='Ship_Name', how='left')
    
    def check_group(group):
        # For each record in A (represented by this group), check all B matches
        for _, row in group.iterrows():
            f_name = str(row.get('First_Name', '')).lower()
            s_name = str(row.get('Surname', '')).lower()
            full_name_a = str(row.get('Name', '')).lower()

            # Skip if B record is empty
            if not f_name and not s_name:
                continue

            # Check if either name component exists in the File A Name string
            if (f_name and f_name in full_name_a) or (s_name and s_name in full_name_a):
                return True # A match was found in B
        
        return False # No match found in B for this person

    # 5. Group by the temporary ID to evaluate each original File A record
    print("Performing comparison...")
    exists_in_b = merged_with_id.groupby('temp_id').apply(check_group)

    # 6. Filter df_a to get only those that DO NOT exist in B
    excel_c = df_a[~exists_in_b.values].drop(columns=['temp_id'])

    # 7. Save to Excel C
    excel_c.to_excel(output_path, index=False)
    print(f"Comparison complete. {len(excel_c)} non-matching records saved to {output_path}")

# Usage
find_missing_records('Book_of_Negroes_Copy.xlsx', 'Black_Loyalist_Directory_Final.xlsx', 'Missing_Records_BON.xlsx')