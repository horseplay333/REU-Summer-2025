import pandas as pd

df = pd.read_pickle("information_events.pkl")

# Add prefixes
df['Source'] = 'src_' + df['Source'].astype(str)
df['Destination'] = 'dest_' + df['Destination'].astype(str)

df['Signal_Name'] = df['Source'].str.replace('src_', '', regex=False)

filtered_rows = []

# Loop through each unique signal name in the Source column
for signal in df['Signal_Name'].unique():
    source_value = f"src_{signal}"
    dest_value = f"dest_shadow_{signal}"

    # Get rows where Destination matches
    matched_rows = df[df['Destination'] == dest_value]

    # Skip this group if all Time values are 0
    if not matched_rows.empty and not (matched_rows['Time'] == 0).all():
        filtered_rows.append(matched_rows)

# all filtered rows into a single DataFrame
filtered_df = pd.concat(filtered_rows, ignore_index=True)

# Print results
for signal in filtered_df['Signal_Name'].unique():
    source_value = f"src_{signal}"
    chunk = filtered_df[filtered_df['Signal_Name'] == signal]
    print(source_value)
    print(chunk.to_string(index=False))
    print()

filtered_df.to_pickle("paths.pkl")


