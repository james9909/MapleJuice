#!/bin/bash

if [[ -z $1 ]]; then
    echo "Usage: $0 <num workers>"
    exit 1
fi

SUFFIX="$(hexdump -n 4 -e '4/4 "%08X" 1 "\n"' /dev/random | xargs)"

master="localhost"
if [[ "$(hostname)" == *illinois.edu ]]; then
    master="$(hostname)"
fi

function run_command() {
    echo "> $1"
    cargo run --release --bin mj-client -q -- --master "$master:3000" -c "$1"
}

run_command "put apps/word_count/data/1.txt word_count_data/1.txt"
run_command "put apps/word_count/data/2.txt word_count_data/2.txt"
run_command "put apps/word_count/data/3.txt word_count_data/3.txt"
run_command "put apps/word_count/data/4.txt word_count_data/4.txt"
run_command "put apps/word_count/data/5.txt word_count_data/5.txt"
run_command "maple apps/word_count/maple.sh 1 word_count_map_$SUFFIX word_count_data"
run_command "juice apps/word_count/juice.sh 1 word_count_map_$SUFFIX word_count_res_$SUFFIX delete_input=true"
