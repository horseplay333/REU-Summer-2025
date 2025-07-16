import pandas as pd

# Load and preprocess
df = pd.read_pickle("paths.pkl")
df = df[df['Time'] != 0].copy()

df['Destination'] = df['Destination'].str.replace('^dest_shadow_', '', regex=True)
df['Source'] = df['Source'].str.replace('^src_', '', regex=True)

df = df.drop(columns=['Signal_Name'])
df = df.sort_values(by='Time').reset_index(drop=True)

matches = []

for i, row1 in df.iterrows():
    dest1 = row1['Destination']
    time1 = row1['Time']
    
    # Find candidate rows where Source == Destination1 and Time > Time1
    matching_rows_2 = df[(df['Source'] == dest1) & (df['Time'] > time1)]
    
    for j, row2 in matching_rows_2.iterrows():
        dest2 = row2['Destination']
        time2 = row2['Time']
        
        # Find candidate rows where Source == Destination2 and Time > Time2
        matching_rows_3 = df[(df['Source'] == dest2) & (df['Time'] > time2)]
        
        for k, row3 in matching_rows_3.iterrows():
            dest3 = row3['Destination']
            time3 = row3['Time']
            
            # Find candidate rows where Source == Destination3 and Time > Time3
            matching_rows_4 = df[(df['Source'] == dest3) & (df['Time'] > time3)]
            
            for _, row4 in matching_rows_4.iterrows():
                matches.append({
                    'Time1': time1, 'Source1': row1['Source'], 'Destination1': dest1,
                    'Time2': time2, 'Source2': row2['Source'], 'Destination2': dest2,
                    'Time3': time3, 'Source3': row3['Source'], 'Destination3': dest3,
                    'Time4': row4['Time'], 'Source4': row4['Source'], 'Destination4': row4['Destination'],
                })

# Convert to DataFrame
quadruplet_matches_df = pd.DataFrame(matches)

# Show result
print(quadruplet_matches_df)


