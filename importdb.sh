#!/bin/bash
echo "importing $1"
mkdir "jsonFiles"
./extractTransform-node13-linux "$1"

for file in "jsonFiles"/*; do
    mongoimport --db pedestrain --collection pedestrain_by_time --jsonArray --batchSize 1 --upsertFields Year,Month,Day,hours --file "$file"
done

