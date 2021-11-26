#!/bin/bash

while read -r line; do
    for word in $line; do
        printf "%s 1\n" "$word"
    done
done < "${1:-/dev/stdin}"
