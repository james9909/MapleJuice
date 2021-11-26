#!/usr/bin/env python3
import sys

for line in sys.stdin:
    line = line.strip()
    v = line.split(",")
    for i in range(len(v)-1):
        for j in range(i+1, len(v)):
            if v[i] < v[j]:
                print(f"{v[i]},{v[j]} 1")
            else:
                print(f"{v[j]},{v[i]} 0")
