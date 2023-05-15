#!/bin/bash 
# script to run latency overhead evaluation with different quorum size for opaxos
# the final results are .dat file containing the latencies and secret-sharing time

source variables.sh
crt_time=$(date +"%Y%m%d%H%M%S")

EXPERIMENTS=(
    "paxos"
    "opaxos shamir"
    "opaxos ssms"
)
QUORUM_SIZE_Q1=(2 3 4 5)
QUORUM_SIZE_Q2=(5 4 3 2)

function kill_prev_instances {
    PRE_CMDS=(
        'sudo killall ./server'                     # kill the previous process, if any, using the process name 'server'
        "sudo fuser -k ${INTERNAL_PORT}/tcp"        # kill the previous process, if any, using the tcp port
        'sudo rm -rf /tmp/rpc_*.sock'               # remove previous socket file, if any
    )

    # killing other server process, if any
    echo "> killing previous instances ..."
    for c in "${PRE_CMDS[@]}"; do
        for n in "${MACHINES[@]}"; do
            ssh -i $SSH_KEY_LOC $SSH_USERNAME@$n "${c} > /dev/null 2>&1" &
        done
        wait
    done
}

function prepare_result_dir {
    mkdir -p results/${crt_time}_quorum
}

# prepare the config file
# $1 is the size of Q1
# $2 is the size of Q2
# $3 is the protocol name (paxos or opaxos)
# $4 is the secret-sharing algorithm (for opaxos)
function prepare_config {
    cfg_loc="results/${crt_time}_quorum/cfg_$3_$4_$1_$2.json"
    cp dist_quorum.template $cfg_loc
    echo "> preparing the configuration file ..."
    echo "   loc: $cfg_loc"

    # insert the internal address
    temp=""; i=1
    for HOST in "${HOSTS[@]}"; do
        temp="$temp\"1.$i\": \"tcp://$HOST:$INTERNAL_PORT\", "
        i=$((i+1))
    done
    temp=${temp%??}
    sed -i "" "s%X_INTERNAL_ADDRESS%${temp}%g" $cfg_loc

    # insert the public address
    temp=""; i=1
    for HOST in "${MACHINES[@]}"; do
        temp="$temp\"1.$i\": \"tcp://$HOST:$PUBLIC_PORT\", "
        i=$((i+1))
    done
    temp=${temp%??}
    sed -i "" "s%X_PUBLIC_ADDRESS%${temp}%g" $cfg_loc

    # insert the roles, the proposer is the first machine
    temp=""; i=1
    for HOST in "${MACHINES[@]}"; do
        if [ $i -eq 1 ]; then
            temp="$temp\"1.$i\": \"proposer,acceptor,learner\", "
        else
            temp="$temp\"1.$i\": \"acceptor,learner\", "
        fi
        i=$((i+1))
    done
    temp=${temp%??}
    sed -i "" "s%X_ROLES%${temp}%g" $cfg_loc

    # insert the value size = 50 bytes
    sed -i "" "s%X_SIZE%50%g" $cfg_loc

    # insert the protocol info
    case $3 in
        paxos)
            temp="\"name\": \"paxos\"";;
        opaxos)
            case $4 in
                shamir)
                    temp="\"name\": \"opaxos\", \"secret_sharing\": \"shamir\", \"threshold\": 2, \"quorum_1\": $1, \"quorum_2\": $2, \"quorum_fast\": 5";;
                ssms)
                    temp="\"name\": \"opaxos\", \"secret_sharing\": \"ssms\", \"threshold\": 2, \"quorum_1\": $1, \"quorum_2\": $2, \"quorum_fast\": 5";;
            esac
            ;;
    esac
    sed -i "" "s%X_PROTOCOL%${temp}%g" $cfg_loc

    # upload the config to all the machines
    for HOST in "${MACHINES[@]}"; do
        scp -q -i $SSH_KEY_LOC $cfg_loc $SSH_USERNAME@$HOST:~/config.json
        ssh -i $SSH_KEY_LOC $SSH_USERNAME@$HOST "sudo mv ~/config.json /usr/local/griya/bin/config.json"
    done
}

# run paxos/opaxos instances in all the machines
# $1 is the protocol name (paxos or opaxos)
function run_instances {
    i=1
    PIDS=()
    ALG=$1

    # run the server (paxos/opaxos instance) in each machine
    echo "> runing instances in all machines ..."
    for HOST in "${MACHINES[@]}"; do
        ID=1.${i}
        
        ssh -i $SSH_KEY_LOC $SSH_USERNAME@$HOST \
            "cd /usr/local/griya/bin; \
            sudo cset shield -e env -- GOGC=100 ./server -id ${ID} -algorithm ${ALG} \
            -client_type unix -client_action block -log_level error -log_stdout \
            -config config.json" \
            &
        
        # store the pid of the ssh background process so we can kill it later
        PIDS+=($!)

        i=$(($i+1))
    done

    # wait until all the instances in all the machines are ready
    sleep 1
}

# run multiple clients in the first machine
# $1 is the protocol name (paxos or opaxos)
function run_clients {
    ALG=$1
    
    echo "> warming up ..."
    ssh -i $SSH_KEY_LOC $SSH_USERNAME@${MACHINES[0]} \
        "cd /usr/local/griya/bin; \
        sudo ./client -id 1.1 -algorithm ${ALG} \
        -client_action block -log_level info -config config.json"
    ssh -i $SSH_KEY_LOC $SSH_USERNAME@${MACHINES[0]} \
        "cd /usr/local/griya/bin; \
        sudo rm latency"
    
    echo "> runing the clients in the first machine and record the latencies ..."
    ssh -i $SSH_KEY_LOC $SSH_USERNAME@${MACHINES[0]} \
        "cd /usr/local/griya/bin; \
        sudo ./client -id 1.1 -algorithm ${ALG} \
        -client_action block -log_level info -config config.json -log_stdout"
}

# $1 is the size of Q1
# $2 is the size of Q2
# $3 is the protocol name (paxos or opaxos)
# $4 is the secret-sharing algorithm (for opaxos)
function gather_results {
    echo "> gathering the latency data ..."
    
    # gather the latency data
    target_loc="results/${crt_time}_quorum/quorum_lat_$3_$4_$1_$2.dat"
    scp -q -i $SSH_KEY_LOC $SSH_USERNAME@${MACHINES[0]}:/usr/local/griya/bin/latency $target_loc
}

# kill the instances in all the machines by killing the ssh background process
function kill_instances {
    for pid in "${PIDS[@]}"; do
        kill $pid
    done
    for n in "${MACHINES[@]}"; do
        ssh -i $SSH_KEY_LOC $SSH_USERNAME@$n \
            "sudo killall ./server > /dev/null 2>&1; \
            cd /usr/local/griya/bin; sudo ./cleanup.sh > /dev/null 2>&1" &
    done
    wait
}

prepare_result_dir

# begin the measurements
for E in "${EXPERIMENTS[@]}"; do
    for i in "${!QUORUM_SIZE_Q1[@]}"; do 
        Q1=${QUORUM_SIZE_Q1[$i]}
        Q2=${QUORUM_SIZE_Q2[$i]}

        echo "=> Run a measurement (protocol: $E, |Q1|: $Q1 , |Q2|: $Q2)"
        kill_prev_instances
        prepare_config $Q1 $Q2 $E
        run_instances $E
        run_clients $E
        gather_results $Q1 $Q2 $E
        kill_instances
        
        echo ""
        sleep 1
    done
done
