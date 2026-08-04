#!/bin/bash

start=`date +%s`
hadoop fs -put /data /input
end=`date +%s`

runtime=$((end-start))

echo ${runtime} >> benchmark_load_data.txt