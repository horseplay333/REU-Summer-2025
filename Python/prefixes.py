from collections import Counter

def count_prefixes_from_file(file_path):
    with open(file_path, 'r') as f:
        content = f.read()

    # Split by any whitespace (space, tab, newline)
    signal_names = content.split()

    # Extract prefix: text before the first underscore
    prefixes = [name.split('_', 1)[0] for name in signal_names]

    # Count prefix occurrences
    prefix_counts = Counter(prefixes)

    # Print sorted prefix counts
    print("Prefix Counts (sorted by frequency):\n")
    for prefix, count in prefix_counts.most_common():
        print(f"{prefix}: {count}")

# 🔁 Replace with the actual path to your file
count_prefixes_from_file("prefixes.txt")
