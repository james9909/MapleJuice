#!/usr/bin/env python3
import sys

output = {}
for line in sys.stdin:
    line = line.strip()
    key, value = line.split(" ")
    if key not in output:
        output[key] = {"ones": 0, "zeroes": 0}
    value = int(value)
    if value == 0:
        output[key]["zeroes"] += 1
    else:
        output[key]["ones"] += 1

for key, counts in output.items():
    a, b = key.split(",")
    if counts["ones"] > counts["zeroes"]:
        # A dominates B
        print(f"{a},{b}")
    else:
        # B dominates A
        print(f"{b},{a}")
