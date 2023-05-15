#!/bin/bash 
# script to update and recompile the code in the machines used for
# the evaluations

source variables.sh

CMDS=(
    'sudo rm -rf /usr/local/griya'
    'sudo mkdir /usr/local/griya'
    "sudo git clone https://${GITHUB_ACCESS_KEY}@github.com/fadhilkurnia/go-opaxos.git /usr/local/griya"
    'cd /usr/local/griya/bin; sudo git checkout final'
    'cd /usr/local/griya/bin; sudo -E env "PATH=$PATH" ./build.sh;'
)

for c in "${CMDS[@]}"; do
    for nid in "${SERVER_MACHINES[@]}"; do
        n="${MACHINES[$nid]}"
        ssh -i $SSH_KEY_LOC $SSH_USERNAME@$n $c
    done
    for cid in "${CLIENT_MACHINES[@]}"; do
        n="${MACHINES[$cid]}"
        ssh -i $SSH_KEY_LOC $SSH_USERNAME@$n $c
    done
done