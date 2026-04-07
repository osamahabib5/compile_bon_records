import pandas as pd
import re

def clean_value(val):
    """
    Removes leading/trailing whitespace and irregular characters from strings.
    """
    if not isinstance(val, str):
        if pd.isna(val): return ""
        val = str(val)
    val = val.strip()
    val = re.sub(r'^[^a-zA-Z0-9]+|[^a-zA-Z0-9]+$', '', val)
    return val.strip()

def split_name(name_str):
    """
    Splits 'Name' into First_Name and Surname. 
    Handles multi-part first names and missing surnames.
    """
    name = clean_value(name_str)
    if not name:
        return "", "-"
    
    parts = name.split()
    if len(parts) > 1:
        # Last part is Surname, everything before is First_Name
        first_name = " ".join(parts[:-1])
        surname = parts[-1]
    else:
        first_name = parts[0]
        surname = "-"
    return first_name, surname

def map_gender(gender_val):
    """
    Maps shorthand gender codes to full descriptive strings.
    """
    mapping = {
        'm': 'Male',
        'f': 'Female',
        'c m': 'Child Male',
        'c f': 'Child Female'
    }
    val = str(gender_val).lower().strip()
    return mapping.get(val, gender_val)

def find_and_consolidate_records(file_a_path, file_b_path, missing_out, consolidated_out):
    print("🚀 Loading and preparing data...")
    df_a = pd.read_excel(file_a_path)
    df_b = pd.read_excel(file_b_path)

    # 1. Standardize File A Column Names (replace spaces with _)
    df_a.columns = [col.replace(' ', '_') for col in df_a.columns]

    # 2. Process File A specific logic (Name splitting and Gender mapping)
    if 'Name' in df_a.columns:
        print("📝 Splitting names in File A...")
        names_split = df_a['Name'].apply(split_name)
        df_a['First_Name'] = [x[0] for x in names_split]
        df_a['Surname'] = [x[1] for x in names_split]

    if 'Gender' in df_a.columns:
        print("🧬 Mapping genders in File A...")
        df_a['Gender'] = df_a['Gender'].apply(map_gender)

    # 3. Preparation for Comparison
    df_a['Ship_Clean'] = df_a['Ship_Name'].apply(clean_value)
    df_b['Ship_Clean'] = df_b['Ship_Name'].apply(clean_value)
    df_a['temp_id'] = range(len(df_a))

    # Merge for comparison logic
    merged = pd.merge(df_a, df_b, left_on='Ship_Clean', right_on='Ship_Clean', how='left', suffixes=('_A', '_B'))
    
    def check_group(group):
        for _, row in group.iterrows():
            f_b = str(row.get('First_Name_B', '')).lower()
            s_b = str(row.get('Surname_B', '')).lower()
            f_a = str(row.get('First_Name_A', '')).lower()
            s_a = str(row.get('Surname_A', '')).lower()

            if not f_b and not s_b: continue
            
            # Match if First Name OR Surname matches
            if (f_b and f_b == f_a) or (s_b and s_b == s_a):
                return True 
        return False

    print("🔍 Searching for missing records...")
    exists_in_b = merged.groupby('temp_id').apply(check_group)
    
    # Identify Missing Records
    missing_records = df_a[~exists_in_b.values].copy()

    # 4. Save Missing Records File (Original File A format but cleaned)
    missing_records_export = missing_records.drop(columns=['temp_id', 'Ship_Clean'])
    missing_records_export.to_excel(missing_out, index=False)

    # 5. Consolidation Logic
    print("📋 Consolidating records into File B schema...")
    
    # We only keep columns from Missing Records that exist in File B
    # This automatically populates Departure_Date and Arrival_Port_City if names match
    b_cols = df_b.columns.tolist()
    missing_aligned = missing_records[missing_records.columns.intersection(b_cols)].copy()
    
    # Ensure missing_aligned has all File B columns (filled with NaN if missing from A)
    for col in b_cols:
        if col not in missing_aligned.columns:
            missing_aligned[col] = pd.NA

    # Combine File B with the aligned missing records
    consolidated_df = pd.concat([df_b, missing_aligned], ignore_index=True)
    
    # Sort by Ship Name so missing records are "under the belt" of the ship
    consolidated_df = consolidated_df.sort_values(by='Ship_Name', ascending=True)

    # Clean up and Save Consolidated File
    if 'Ship_Clean' in consolidated_df.columns:
        consolidated_df = consolidated_df.drop(columns=['Ship_Clean'])
        
    consolidated_df.to_excel(consolidated_out, index=False)

    print(f"✅ Success!")
    print(f"   - Total records from A missing in B: {len(missing_records)}")
    print(f"   - New consolidated file size: {len(consolidated_df)}")

# Execution
if __name__ == "__main__":
    find_and_consolidate_records(
        'Book_of_Negroes_Copy.xlsx', 
        'Black_Loyalist_Directory_Final.xlsx', 
        'Missing_Records_BON.xlsx', 
        'Consolidated_Directory.xlsx'
    )