#!/bin/bash

key="$1"

count=0
while read -r line; do
	((count=count+1))
done < "/dev/stdin"

printf "%s %d\n" "$key" "$count"
