#!/bin/bash 
# script to initialize machines for the evaluations

source variables.sh

CMDS=()
CMD_DESC=()
CMD_DESC+=("update packages");                     CMDS+=("sudo apt-get update")
# CMD_DESC+=("install cpuset");                      CMDS+=('sudo apt-get install -y -q cpuset')
# CMD_DESC+=("reset cpuset");                        CMDS+=('sudo cset shield --reset' )
# CMD_DESC+=("reserve cpu cores for opaxos");        CMDS+=("sudo cset shield -c ${CORE_ID} -k on")
CMD_DESC+=("download golang");                     CMDS+=("wget --quiet --no-clobber ${GO_DOWNLOAD_URL}")
CMD_DESC+=("remove previous go installation");     CMDS+=('sudo rm -rf /usr/local/go')
CMD_DESC+=("remove previous working directory");   CMDS+=("sudo rm -rf /usr/local/griya")
CMD_DESC+=("extract golang installation");         CMDS+=("sudo tar -C /usr/local -xzf ${GO_DOWNLOAD_FILE}")
CMD_DESC+=("include golang's bin in PATH");        CMDS+=('echo "export PATH=$PATH:/usr/local/go/bin" | sudo tee -a /etc/environment')
CMD_DESC+=("prepare the working directory");       CMDS+=('sudo mkdir /usr/local/griya')
CMD_DESC+=("clone the source-code from github");   CMDS+=("sudo git clone --quiet https://${GITHUB_ACCESS_KEY}@github.com/fadhilkurnia/go-opaxos.git /usr/local/griya")
CMD_DESC+=("change the working branch");           CMDS+=("cd /usr/local/griya/bin; sudo git checkout --quiet ${MAIN_BRANCH}")
CMD_DESC+=("build opaxos from the source-code");   CMDS+=('cd /usr/local/griya/bin; echo $PATH; sudo -E env "PATH=$PATH" bash ./build_silent.sh')

# checking connection from the first node to all other nodes
echo "==> Checking the connection from the first server machine to the remaining server machines ..."
first_server_machine="${MACHINES[${SERVER_MACHINES[0]}]}"
for hid in "${SERVER_MACHINES[@]}"; do
    echo "     ${first_server_machine} to ${HOSTS[$hid]}"
    
    # run the ping command, only print out if error occured
    h="${HOSTS[$hid]}"
    output=`ssh -o StrictHostKeyChecking=no -i $SSH_KEY_LOC $SSH_USERNAME@${first_server_machine} "ping $h -c 1"`
    if [ $? -ne 0 ]; then
        echo $output
        echo "    Error: there is no connection between ${first_server_machine}(${HOSTS[${SERVER_MACHINES[0]}]}) and $h, try to restart the cluster."
        exit $?
    fi
done

echo "==> Initializing all the server machines"
for i in "${!CMD_DESC[@]}"; do
    desc=${CMD_DESC[$i]}
    cmd=${CMDS[$i]}
    echo "     $desc ..."
    for hid in "${SERVER_MACHINES[@]}"; do
        n=(${MACHINES[$hid]})
        output=`ssh -o StrictHostKeyChecking=no -i $SSH_KEY_LOC $SSH_USERNAME@$n $cmd` || echo "$n | $output"
    done
done

echo "==> Initializing all the client machines"
for i in "${!CMD_DESC[@]}"; do
    desc=${CMD_DESC[$i]}
    cmd=${CMDS[$i]}
    echo "     $desc ..."
    for hid in "${CLIENT_MACHINES[@]}"; do
        CLIENT_MACHINE=(${MACHINES[$hid]})
        output=`ssh -o StrictHostKeyChecking=no -i $SSH_KEY_LOC $SSH_USERNAME@$CLIENT_MACHINE $cmd` || echo "$CLIENT_MACHINE | $output"
    done
done
# unassigned the core for the consensus instance in the client machine so we
# can use all the cores for the clients
# echo "     reset cpu assignment in the client machine"
# for hid in "${CLIENT_MACHINES[@]}"; do
#     CLIENT_MACHINE=(${MACHINES[$hid]})
#     output=`ssh -o StrictHostKeyChecking=no -i $SSH_KEY_LOC $SSH_USERNAME@$CLIENT_MACHINE 'sudo cset shield --reset'` || echo "$CLIENT_MACHINE | $output"
# done

