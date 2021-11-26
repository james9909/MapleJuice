#!/usr/bin/env python3

import sys

key = sys.argv[1]
a, b = key.split(",")

n_ones = 0
n_zeroes = 0

for line in sys.stdin:
    n = int(line.strip())
    if n == 0:
        n_zeroes += 1
    else:
        n_ones += 1

if n_ones > n_zeroes:
    # A dominates B
    print(f"{a},{b}")
else:
    # B dominates A
    print(f"{b},{a}")
