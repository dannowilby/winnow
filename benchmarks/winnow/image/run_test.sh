#!/bin/bash

start=`date +%s`
./winnow_cli run-job
end=`date +%s`

runtime=$((end-start))

echo ${runtime} >> benchmark_run_test.txt