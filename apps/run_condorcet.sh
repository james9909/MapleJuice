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
    cargo run --release --bin mj-client -q -- --master "$master:3000" -c "$1" 2> /dev/null
}

run_command "put apps/condorcet/data/votes_1.txt condorcet_data/votes_1.txt"
run_command "put apps/condorcet/data/votes_2.txt condorcet_data/votes_2.txt"
run_command "put apps/condorcet/data/votes_3.txt condorcet_data/votes_3.txt"
run_command "put apps/condorcet/data/votes_4.txt condorcet_data/votes_4.txt"
run_command "put apps/condorcet/data/votes_5.txt condorcet_data/votes_5.txt"
run_command "put apps/condorcet/data/votes_6.txt condorcet_data/votes_6.txt"
run_command "put apps/condorcet/data/votes_7.txt condorcet_data/votes_7.txt"
run_command "put apps/condorcet/data/votes_8.txt condorcet_data/votes_8.txt"
run_command "put apps/condorcet/data/votes_9.txt condorcet_data/votes_9.txt"

run_command "maple apps/condorcet/maplejuice/maple1.py $1 condorcet_map1_$SUFFIX condorcet_data";
run_command "juice apps/condorcet/maplejuice/juice1.py $1 condorcet_map1_$SUFFIX condorcet_output_1_$SUFFIX/result";
run_command "maple apps/condorcet/maplejuice/maple2.py $1 condorcet_map2_$SUFFIX condorcet_output_1_$SUFFIX";
run_command "juice apps/condorcet/maplejuice/juice2.py $1 condorcet_map2_$SUFFIX condorcet_final_output_$SUFFIX";
