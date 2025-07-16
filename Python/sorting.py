import pandas as pd
import re

def parse_iflow_txt(file_path):
    records = []
    current_source = None

    with open(file_path, 'r') as file:
        for line in file:
            line = line.strip()
            if not line:
                continue

            # Remove any brackets and colons
            line = re.sub(r"[\[\]\{\}:]", "", line)

            # Check for a source name in single quotes
            match = re.match(r"'([^']+)'", line)
            if match:
                current_source = match.group(1)
                continue 

            # Destination and Time
            if current_source and not line.startswith("dtype"):
                try:
                    destination, time_str = line.rsplit(None, 1)
                    time = int(time_str)
                    records.append({
                        "Destination": destination,
                        "Source": current_source,
                        "Time": time
                    })
                except ValueError:
                    continue
    return pd.DataFrame(records)

# Usage
df = parse_iflow_txt("iflow_times2.txt")
df = df.sort_values(by="Time", ascending=True)
df = df.reset_index(drop=True)

print(df)
df.to_pickle("information_events.pkl")


