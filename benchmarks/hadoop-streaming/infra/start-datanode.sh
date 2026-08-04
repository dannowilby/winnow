#!/bin/bash
set -euxo pipefail
exec > /var/log/user-data.log 2>&1

su - hadoop -c 'hdfs --daemon start datanode'
su - hadoop -c 'yarn --daemon start nodemanager'
