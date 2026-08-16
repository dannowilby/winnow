#!/bin/bash

# $1 is the instance id
# $2 is the eice id
# $3 is the ip addr

# remove any old ssh key entries for the machine
ssh-keygen -R $3

# connect to the instance via ssh
aws ec2-instance-connect send-ssh-public-key   --instance-id $1   --instance-os-user ubuntu   --ssh-public-key file:///tmp/eic_key.pub && ssh -i /tmp/eic_key   -o ProxyCommand="aws ec2-instance-connect open-tunnel --instance-id $1 --instance-connect-endpoint-id $2 --private-ip-address $3 --remote-port 22" ubuntu@$3