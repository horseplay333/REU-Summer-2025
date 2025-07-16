import pickle
import pandas as pd
from collections import defaultdict, deque
from typing import List, Tuple, Set

def load_paths(file_path: str) -> pd.DataFrame:
    with open(file_path, 'rb') as f:
        df = pickle.load(f)
    return df

from collections import defaultdict, deque
from typing import List, Tuple, Set
import pandas as pd


def build_graph(df: pd.DataFrame) -> Tuple[dict, Set[Tuple[str, int]]]:
    graph = defaultdict(list)
    all_sources = set()
    all_destinations = set()

    for _, row in df.iterrows():
        src = row['Source']
        dst = row['Destination']
        t = row['Time']
        
        # Make everything source-relative and capture the timestamped relationship
        graph[(src, t)].append((dst, t))
        all_sources.add((src, t))
        all_destinations.add((dst, t))

    # Initial taint nodes: sources that are never destinations
    initial_taint_nodes = all_sources - all_destinations
    return graph, initial_taint_nodes


def propagate_taint(graph: dict,
                    initial_taint: Set[Tuple[str, int]]) -> Tuple[List[int], List[List[Tuple[str, str]]]]:
    taint_by_time = defaultdict(set) 
    visited = set()
    queue = deque(initial_taint)

    for reg, t in initial_taint:
        # Initial taint sources (no incoming edge)
        taint_by_time[t].add((None, reg))
        visited.add((reg, t))

    while queue:
        reg, t = queue.popleft()
        for dst, t_dst in graph.get((reg, t), []):
            if (dst, t_dst) not in visited:
                taint_by_time[t_dst].add((reg, dst))
                queue.append((dst, t_dst))
                visited.add((dst, t_dst))

    # Define a sort key that safely handles None
    def sort_key(pair: Tuple[str, str]) -> Tuple[str, str]:
        src, dst = pair
        return (src if src is not None else "", dst)

    sorted_times = sorted(taint_by_time.keys())
    taint_flows = [sorted(taint_by_time[t], key=sort_key) for t in sorted_times]
    return sorted_times, taint_flows

if __name__ == "__main__":
    path_df = load_paths("paths.pkl")

    taint_graph, inferred_initial_taint = build_graph(path_df)

    print("First taint sources:")
    for src, t in sorted(inferred_initial_taint):
        print(f"  t={t}: {src}")

    # Save initial taint sources to pickle
    with open("initial_sources.pkl", "wb") as f:
        pickle.dump(inferred_initial_taint, f)

    # Run taint propagation
    times, flows = propagate_taint(taint_graph, inferred_initial_taint)

    print("\n--- Tainted @ Time ---")
    for t, regs in zip(times, flows):
        print(f"t={t}: {regs}")

    # Save sparse matrix (taint flows by time) to pickle
    sparse_matrix = list(zip(times, flows)) 
    with open("sparse_matrix.pkl", "wb") as f:
        pickle.dump(sparse_matrix, f)
