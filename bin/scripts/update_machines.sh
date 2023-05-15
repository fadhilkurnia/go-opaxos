#!/bin/bash 
# script to update and recompile the code in the machines used for
# the evaluations

source variables.sh

CMDS=()
CMD_DESC=()
CMD_DESC+=("remove prev files");                CMDS+=("sudo rm -rf /usr/local/griya")
CMD_DESC+=("create new dir");                   CMDS+=("sudo mkdir /usr/local/griya")
CMD_DESC+=("download the latest source code");  CMDS+=("sudo git clone https://${GITHUB_ACCESS_KEY}@github.com/fadhilkurnia/go-opaxos.git /usr/local/griya")
CMD_DESC+=("change to the target branch");      CMDS+=("cd /usr/local/griya/bin; sudo git checkout --quiet ${MAIN_BRANCH}")
CMD_DESC+=("compile the source code");          CMDS+=('cd /usr/local/griya/bin; sudo -E env "PATH=$PATH" ./build.sh;')


echo "==> Updating all the server and client machines"
for i in "${!CMD_DESC[@]}"; do
    desc=${CMD_DESC[$i]}
    cmd=${CMDS[$i]}
    echo "    * $desc ..."
    for nid in "${SERVER_MACHINES[@]}"; do
        n="${MACHINES[$nid]}"
        ssh -i $SSH_KEY_LOC $SSH_USERNAME@$n $cmd
    done
    for cid in "${CLIENT_MACHINES[@]}"; do
        n="${MACHINES[$cid]}"
        ssh -i $SSH_KEY_LOC $SSH_USERNAME@$n $cmd
    done
done