import pandas as pd
import xlsxwriter
import os

# --- SETTINGS ---
INPUT_FILE = 'Ancestors Database_v01_copy.xlsx'  
REF_FILE = 'uscounties.csv'
OUTPUT_FILE = 'Genealogy_Smart_Entry_v3.xlsx'

def create_smart_genealogy_excel():
    print("🚀 Initializing data processing...")

    # 1. Load Reference Data
    if not os.path.exists(REF_FILE):
        print(f"❌ Error: {REF_FILE} not found.")
        return
    
    geo_df = pd.read_csv(REF_FILE)
    # Standardize data for matching
    geo_df['state_id'] = geo_df['state_id'].astype(str).str.upper().str.strip()
    geo_df['county'] = geo_df['county'].astype(str).str.strip()
    
    # Prepare Sheet2 structure: County(A), State(B), Lat(C), Lng(D), LookupKey(E), Coords(F)
    geo_df = geo_df[['county', 'state_id', 'lat', 'lng']].copy()
    geo_df['Lookup_Key'] = geo_df['state_id'] + "-" + geo_df['county']
    geo_df['Formatted_Coords'] = geo_df['lat'].astype(str) + ", " + geo_df['lng'].astype(str)
    
    # Crucial: Sort by state then county so named ranges are contiguous
    geo_df = geo_df.sort_values(['state_id', 'county']).reset_index(drop=True)
    unique_states = sorted(geo_df['state_id'].unique().tolist())

    # 2. Load Input Genealogy File
    if not os.path.exists(INPUT_FILE):
        print(f"❌ Error: {INPUT_FILE} not found.")
        return
    
    df_orig = pd.read_excel(INPUT_FILE) if INPUT_FILE.endswith('.xlsx') else pd.read_csv(INPUT_FILE)

    # 3. Construct Final DataFrame and Track Column Roles
    # We use internal unique keys to manage columns but will name them simply in Excel
    new_data = {}
    display_headers = []
    
    col_idx = 0
    for col in df_orig.columns:
        # Keep original column
        col_key = f"orig_{col_idx}"
        new_data[col_key] = df_orig[col]
        display_headers.append(col)
        
        # Inject Military columns next to Names
        if any(k in col for k in ["Surname", "Last Name"]):
            new_data[f"mil_{col_idx}"] = ["No"] * len(df_orig)
            display_headers.append("Military_Personnel")
            new_data[f"war_{col_idx}"] = ["-"] * len(df_orig)
            display_headers.append("War")
            
        # Inject Geo columns next to Location
        if "City, County, State" in col:
            new_data[f"state_{col_idx}"] = [""] * len(df_orig)
            display_headers.append("State")
            new_data[f"county_{col_idx}"] = [""] * len(df_orig)
            display_headers.append("County")
            new_data[f"coords_{col_idx}"] = [""] * len(df_orig)
            display_headers.append("Coordinates")
            
        col_idx += 1

    df_final = pd.DataFrame(new_data)

    # 4. Write to Excel
    writer = pd.ExcelWriter(OUTPUT_FILE, engine='xlsxwriter')
    df_final.to_excel(writer, index=False, sheet_name='Sheet1')
    geo_df.to_excel(writer, index=False, sheet_name='Sheet2')
    
    workbook = writer.book
    sheet1 = writer.sheets['Sheet1']
    sheet2 = writer.sheets['Sheet2']
    
    # Overwrite headers with the clean user-facing names
    for i, name in enumerate(display_headers):
        sheet1.write(0, i, name)

    # 5. Define Named Ranges for Dependent Dropdowns
    # Unique States list in Sheet2 Column Z for the primary dropdown
    sheet2.write_column('Z1', unique_states)
    state_list_ref = f"=Sheet2!$Z$1:$Z${len(unique_states)}"
    
    for state in unique_states:
        # Find start and end rows for this state's counties in the sorted Sheet2
        state_rows = geo_df.index[geo_df['state_id'] == state].tolist()
        if state_rows:
            start_row = state_rows[0] + 2 # +1 header, +1 Excel indexing
            end_row = state_rows[-1] + 2
            # Define name (e.g., S_VA) pointing to Counties in Column A of Sheet2
            workbook.define_name(f"S_{state}", f'=Sheet2!$A${start_row}:$A${end_row}')

    # 6. Apply Interactive Rules
    mil_opts = ['Yes', 'No']
    war_opts = ['Revolutionary', 'Civil', 'World_War_I', 'World_War_II', '-']
    num_rows = len(df_final)

    for i, col_key in enumerate(df_final.columns):
        # Military & War Dropdowns
        if col_key.startswith("mil_"):
            sheet1.data_validation(1, i, num_rows, i, {'validate': 'list', 'source': mil_opts})
        elif col_key.startswith("war_"):
            sheet1.data_validation(1, i, num_rows, i, {'validate': 'list', 'source': war_opts})
            
        # State Selection
        elif col_key.startswith("state_"):
            sheet1.data_validation(1, i, num_rows, i, {'validate': 'list', 'source': state_list_ref})
            
        # Dependent County Selection
        elif col_key.startswith("county_"):
            state_col = xlsxwriter.utility.xl_col_to_name(i - 1)
            for r in range(1, num_rows + 1):
                state_cell = f"{state_col}{r+1}"
                # If State is blank, INDIRECT("S_") is invalid, making dropdown empty
                sheet1.data_validation(r, i, r, i, {
                    'validate': 'list', 
                    'source': f'=INDIRECT("S_"&{state_cell})'
                })
                
        # Coordinate Lookup
        elif col_key.startswith("coords_"):
            s_col = xlsxwriter.utility.xl_col_to_name(i - 2)
            c_col = xlsxwriter.utility.xl_col_to_name(i - 1)
            for r in range(1, num_rows + 1):
                s_ref = f"{s_col}{r+1}"
                c_ref = f"{c_col}{r+1}"
                # VLOOKUP(State-County, Sheet2!E:F, 2, False)
                formula = f'=IFERROR(VLOOKUP({s_ref}&"-"&{c_ref}, Sheet2!$E:$F, 2, FALSE), "")'
                sheet1.write_formula(r, i, formula)

    writer.close()
    print(f"✨ Success! Saved as: {OUTPUT_FILE}")

if __name__ == "__main__":
    create_smart_genealogy_excel()