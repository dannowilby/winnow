#!/bin/bash
set -euxo pipefail   # -e: stop on error (your current script hides failures)

sudo apt-get -y update
sudo apt-get install -y unzip

# --- dedicated user: HDFS/YARN daemons refuse to run as root cleanly ---
sudo useradd -m -s /bin/bash winnow

# copy test scripts
sudo cp /tmp/load_data.sh /tmp/run_test.sh /tmp/verify_results.sh /home/winnow/

# download data
sudo mkdir /home/winnow/sources
sudo curl -o /home/winnow/sources/data.txt https://verify-winnow-data.s3.us-west-1.amazonaws.com/data.txt

# copy application
sudo cp /tmp/winnow /tmp/winnow_cli /home/winnow

# copy job files
sudo cp /tmp/cluster.json /tmp/job.json /tmp/reducer.wasm /tmp/mapper.wasm /tmp/partitioner.wasm /tmp/reader.wasm /home/winnow/

sudo chown -R winnow:winnow /home/winnow
sudo chmod +x /home/winnow/winnow /home/winnow/winnow_cli /home/winnow/load_data.sh /home/winnow/run_test.sh /home/winnow/verify_results.sh
