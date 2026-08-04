
#!/bin/bash

start=`date +%s`
python main.py
end=`date +%s`

runtime=$((end-start))

echo ${runtime} >> benchmark_run_test.txt