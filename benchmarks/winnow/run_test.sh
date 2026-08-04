#!/bin/bash

start=`date +%s`
hadoop jar hadoop-streaming.jar \
    -input input \
    -output output \
    -mapper mapper.py \
    -reducer reducer.py
end=`date +%s`

runtime=$((end-start))

echo ${runtime} >> benchmark_run_test.txt