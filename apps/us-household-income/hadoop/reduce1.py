#!/usr/bin/env python3
import sys


incomes = {
    1: 'Less than $5,000',
    2: '$5,000 to $7,499',
    3: '$7,500 to $9,999',
    4: '$10,000 to $12,499',
    5: '$12,500 to $14,999',
    6: '$15,000 to $19,999',
    7: '$20,000 to $24,999',
    8: '$25,000 to $29,999',
    9: '$30,000 to $34,999',
    10: '$35,000 to $39,999',
    11: '$40,000 to $49,999',
    12: '$50,000 to $59,999',
    13: '$60,000 to $74,999',
    14: '$75,000 to $99,99',
    15: '$100,000 to $149,999',
    16: '$150,000 and over',
}

output = {}

for line in sys.stdin:
    line = line.strip()
    key, value = line.split(" ")
    if key not in output:
        output[key] = 0

    output[key] += 1

for key, total in output.items():
    print(incomes[int(key)], " := ", total)