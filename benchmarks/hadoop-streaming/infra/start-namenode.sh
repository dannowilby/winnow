#!/bin/bash
set -euxo pipefail
exec > /var/log/user-data.log 2>&1

su - hadoop -c 'hdfs namenode -format -force'
su - hadoop -c 'hdfs --daemon start namenode'
su - hadoop -c 'yarn --daemon start resourcemanager'
su - hadoop -c 'mapred --daemon start historyserver'
