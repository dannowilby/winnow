#!/bin/bash
set -euxo pipefail   # -e: stop on error (your current script hides failures)

# --- install java for hadoop ---
sudo apt-get -y update
sudo apt-get -y install openjdk-17-jdk
sudo apt-get -y unzip

# --- install python for mapreduce job ---
sudo curl -LsSf https://astral.sh/uv/install.sh | sudo sh
# installer drops uv in root's ~/.local/bin, which isn't on sudo's secure_path,
# so a later `sudo uv ...` can't find it; move it somewhere sudo can see
sudo mv /root/.local/bin/uv /usr/local/bin/uv
sudo uv python install --default
# uv installs the interpreter under root's HOME; symlink into /usr/local/bin so
# the `hadoop` user (who actually runs the streaming tasks) has python3 on PATH too
sudo ln -sf "$(sudo uv python find)" /usr/local/bin/python3

# --- dedicated user: HDFS/YARN daemons refuse to run as root cleanly ---
sudo useradd -m -s /bin/bash hadoop

# --- unpack into /opt, not a user home ---
sudo tar xzf /tmp/hadoop-3.5.0.tar.gz -C /opt   # dropped the 'v'; it spams the build log
sudo ln -s /opt/hadoop-3.5.0 /opt/hadoop
sudo chown -R hadoop:hadoop /opt/hadoop-3.5.0

# --- HDFS storage dirs (point at instance-store NVMe if you have it) ---
sudo mkdir -p /data/hdfs/name /data/hdfs/data
sudo chown -R hadoop:hadoop /data/hdfs

# --- environment for every shell/user ---
sudo tee /etc/profile.d/hadoop.sh > /dev/null <<'EOF'
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export HADOOP_HOME=/opt/hadoop
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
EOF

# --- also set JAVA_HOME inside hadoop-env.sh (belt and suspenders) ---
echo 'export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64' \
  | sudo tee -a /opt/hadoop/etc/hadoop/hadoop-env.sh > /dev/null

# --- drop in the config files ---
sudo cp /tmp/core-site.xml /tmp/hdfs-site.xml /tmp/yarn-site.xml /tmp/mapred-site.xml \
  /opt/hadoop/etc/hadoop/
sudo chown -R hadoop:hadoop /opt/hadoop/etc/hadoop

# --- drop in the test files ---
sudo cp /tmp/load_data.sh /tmp/run_test.sh /tmp/verify_results.sh /tmp/mapper.py /tmp/reducer.py \
  /home/hadoop/
sudo chown -R hadoop:hadoop /home/hadoop