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

run_command "put apps/us-household-income/data/x00 us_hh_income_data/data_1.txt"
run_command "put apps/us-household-income/data/x01 us_hh_income_data/data_2.txt"
run_command "put apps/us-household-income/data/x02 us_hh_income_data/data_3.txt"
run_command "put apps/us-household-income/data/x03 us_hh_income_data/data_4.txt"
run_command "put apps/us-household-income/data/x04 us_hh_income_data/data_5.txt"
run_command "put apps/us-household-income/data/x05 us_hh_income_data/data_6.txt"
run_command "put apps/us-household-income/data/x06 us_hh_income_data/data_7.txt"
run_command "put apps/us-household-income/data/x07 us_hh_income_data/data_8.txt"
run_command "put apps/us-household-income/data/x08 us_hh_income_data/data_9.txt"
run_command "maple apps/us-household-income/maplejuice/maple1.py $1 us_hh_income_map1_$SUFFIX us_hh_income_data"
run_command "juice apps/us-household-income/maplejuice/juice1.py $1 us_hh_income_map1_$SUFFIX us_hh_income_result_$SUFFIX"
