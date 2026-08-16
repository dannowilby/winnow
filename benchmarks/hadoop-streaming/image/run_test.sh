#!/bin/bash

start=`date +%s`
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -D mapreduce.job.reduces=20 \
    -D mapreduce.input.fileinputformat.input.dir.recursive=true \
    -files mapper.py,reducer.py \
    -input /input \
    -output /output \
    -mapper mapper.py \
    -reducer reducer.py
end=`date +%s`

runtime=$((end-start))

echo ${runtime} >> benchmark_run_test.txt