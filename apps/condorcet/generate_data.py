import sys
import random

num_files = int(sys.argv[1])

CANDIDATES = ["a", "b", "c"]

VOTES_PER_FILE = 500000

for x in range(1, num_files+1):
    with open(f"data/votes_{x}.txt", "w") as f:
        for _ in range(VOTES_PER_FILE):
            random.shuffle(CANDIDATES)
            f.write(f"{','.join(CANDIDATES)}\n")
