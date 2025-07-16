import pandas as pd

# Load and preprocess
df = pd.read_pickle("paths.pkl")
df = df[df['Time'] != 0].copy()

df['Destination'] = df['Destination'].str.replace('^dest_shadow_', '', regex=True)
df['Source'] = df['Source'].str.replace('^src_', '', regex=True)

df = df.drop(columns=['Signal_Name'])
df = df.sort_values(by='Time').reset_index(drop=True)

# Collect matching row pairs
matches = []

for i, row1 in df.iterrows():
    dest = row1['Destination']
    time1 = row1['Time']
    
    # Find rows where Source == Destination and Time is greater
    matching_rows = df[(df['Source'] == dest) & (df['Time'] > time1)]
    
    for _, row2 in matching_rows.iterrows():
        matches.append({
            'Time1': row1['Time'], 'Source1': row1['Source'], 'Destination1': row1['Destination'],
            'Time2': row2['Time'], 'Source2': row2['Source'], 'Destination2': row2['Destination']
        })

# Convert to DataFrame
pair_matches_df = pd.DataFrame(matches)

# Show result
print(pair_matches_df)
pair_matches_df.to_pickle("paths_len3.pkl")