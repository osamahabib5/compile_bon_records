import pandas as pd

def calculate_birth_year(file_path, output_path):
    # 1. Load the Excel file
    df = pd.read_excel(file_path)

    # 2. Perform calculation on temporary series to keep originals untouched
    # We use pd.to_datetime and pd.to_numeric only for the math
    temp_dates = pd.to_datetime(df['Enlistment_date'], errors='coerce')
    temp_ages = pd.to_numeric(df['Age'], errors='coerce')

    # 3. Calculate the year
    temp_birth_year = temp_dates.dt.year - temp_ages

    # 4. Create the 'Birthdate' column with '-' for missing values
    # We use .apply to format the result as a clean string year
    df['Birthdate'] = temp_birth_year.apply(
        lambda x: str(int(x)) if pd.notnull(x) else '-'
    )

    # 5. Move 'Birthdate' next to 'Birth_coordinates'
    cols = df.columns.tolist()
    if 'Birth_coordinates' in cols:
        idx = cols.index('Birth_coordinates')
        # Remove from end, insert at idx + 1
        birthdate_col = cols.pop(cols.index('Birthdate'))
        cols.insert(idx + 1, birthdate_col)
        df = df[cols]

    # 6. Save the file
    # The original Enlistment_date and Age columns remain in their original state
    df.to_excel(output_path, index=False)
    print(f"File saved to: {output_path}")

# Run the function
calculate_birth_year('USCTs_Connecticut_rev_04_COPY.xlsx', 'USCTs_Connecticut_rev_05.xlsx')