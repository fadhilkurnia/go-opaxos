#!/bin/bash 
# script to run any linux command in all the machines
# usage:    ./run_cmd.sh "<cmd>"
# example:  ./run_cmd.sh "sudo fuser -k 1735/tcp"

source variables.sh

for HOST in "${MACHINES[@]}"; do
    ssh -i $SSH_KEY_LOC $SSH_USERNAME@$HOST $1
done