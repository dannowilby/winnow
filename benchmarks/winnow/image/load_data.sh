#!/bin/bash

# generates the keyfile as this is not done automatically

start=`date +%s`

FILENAME=./sources/data.txt
FILE_LENGTH=$(du -sb $FILENAME | awk '{ print $1 }')
KEY_DATA_SIZE=64000000
COUNTER=0

while [[ $COUNTER -lt $FILE_LENGTH ]] 
do
    PROGRESS=$((100 * COUNTER / FILE_LENGTH))
    echo "Generating: ${PROGRESS}%"
    echo "data.txt-${COUNTER}" >> keys.txt
    COUNTER=$((COUNTER + KEY_DATA_SIZE))
done

echo "Generating: 100%"

end=`date +%s`

runtime=$((end-start))

echo ${runtime} >> benchmark_load_data.txt