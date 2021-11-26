#!/usr/bin/env python3
import sys
import csv

data = sys.stdin.readlines()

for line in csv.reader(data):
    if int(line[5]) != -1 and int(line[1]) == 2:
        print(line[5], line[0])
