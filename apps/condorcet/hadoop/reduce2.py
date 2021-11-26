#!/usr/bin/env python3
import sys

# Number of candidates
M = 3

counts = {}
for line in sys.stdin:
    line = line.strip()
    _, value = line.split(" ")
    a, b = value.split(",")
    counts[a] = counts.get(a, 0) + 1

max_count = max(counts.values())
max_candidates = [c for c in counts if counts[c] == max_count]
print(f"Winner: {','.join(max_candidates)}")
