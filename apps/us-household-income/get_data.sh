#!/bin/bash

wget https://www2.census.gov/programs-surveys/cps/datasets/2020/march/asecpub20csv.zip
unzip asecpub20csv.zip -d data
rm asecpub20csv.zip
sed -i '1d' data/hhpub20.csv
shuf -o data/hhpub20.csv < data/hhpub20.csv
split data/hhpub20.csv -n l/9 -d
mv x* data/.
