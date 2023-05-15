#!/bin/bash 
# script to run latency overhead evaluation under bandwidth constraint (1Gbps)
# this script is exactly the same with run_lat_eval.sh, it only differ in line 61
# where we use control network in the cloudlab cluster.
# the final results are .dat file containing the latencies and secret-sharing time

source variables.sh
crt_time=$(date +"%Y%m%d%H%M%S")

EXPERIMENTS=(
    "paxos"
    "opaxos shamir"
    "opaxos ssms"
)
VAL_SIZES=(50 100 1000 10000 25000 50000 100000)
NUM_REQUEST=10000

# by default we run the client in the first machine
CLIENT=${MACHINES[0]}

# uncomment the line below to run the client in a separate machine instead
# CLIENT=$CLIENT_MACHINE

function kill_prev_instances {
    PRE_CMDS=(
        'sudo killall ./server'                     # kill the previous process, if any, using the process name 'server'
        "sudo fuser -k ${INTERNAL_PORT}/tcp"        # kill the previous process, if any, using the tcp port
        'sudo rm -rf /tmp/rpc_*.sock'               # remove previous socket file, if any
        'sudo rm -rf /tmp/paxi_*'                   # remove previous persistent storage
    )

    # killing other server process, if any
    echo "> killing previous instances ..."
    kill_prev_pids=""
    for c in "${PRE_CMDS[@]}"; do
        for n in "${MACHINES[@]}"; do
            ssh -i $SSH_KEY_LOC $SSH_USERNAME@$n "${c} > /dev/null 2>&1" &
            kill_prev_pids="$kill_prev_pids $!"
        done
    done
    wait $kill_prev_pids
}

function prepare_result_dir {
    mkdir -p results/${crt_time}_latency_bw
}

# prepare the config file
# $1 is the value size
# $2 is the protocol name (paxos or opaxos)
# $3 is the secret-sharing algorithm (for opaxos)
function prepare_config {
    cfg_loc="results/${crt_time}_latency_bw/cfg_$2_$3_$1.json"
    warmup_cfg_loc="results/${crt_time}_latency_bw/cfg_$2_$3_$1_warmup.json"
    cp dist_latency.template $cfg_loc
    echo "> preparing the configuration file ..."
    echo "   loc: $cfg_loc"

    # insert the internal address
    temp=""; i=1
    for HOST in "${MACHINES[@]}"; do
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

    # insert the value size
    sed -i "" "s%X_SIZE%${1}%g" $cfg_loc

    # insert the number of request
    sed -i "" "s%X_NUM_REQUEST%${NUM_REQUEST}%g" $cfg_loc

    # insert the protocol info
    case $2 in
        paxos)
            temp="\"name\": \"paxos\"";;
        opaxos)
            case $3 in
                shamir)
                    temp="\"name\": \"opaxos\", \"secret_sharing\": \"shamir\", \"threshold\": 2, \"quorum_1\": 4, \"quorum_2\": 3, \"quorum_fast\": 4";;
                ssms)
                    temp="\"name\": \"opaxos\", \"secret_sharing\": \"ssms\", \"threshold\": 2, \"quorum_1\": 4, \"quorum_2\": 3, \"quorum_fast\": 4";;
            esac
            ;;
    esac
    sed -i "" "s%X_PROTOCOL%${temp}%g" $cfg_loc

    # insert duration time for the warmup config
    sed "s%X_DURATION%5%g" $cfg_loc > $warmup_cfg_loc
    sed -i "" "s%X_DURATION%0%g" $cfg_loc

    # upload the config to all the machines (including client machine)
    upload_pids=""
    for HOST in "${MACHINES[@]}"; do
        scp -q -i $SSH_KEY_LOC $cfg_loc $SSH_USERNAME@$HOST:~/config.json &
        upload_pids="$upload_pids $!"
    done
    scp -q -i $SSH_KEY_LOC $cfg_loc $SSH_USERNAME@$CLIENT_MACHINE:~/config.json &
    upload_pids="$upload_pids $!"
    scp -q -i $SSH_KEY_LOC $warmup_cfg_loc $SSH_USERNAME@$CLIENT_MACHINE:~/warmup_config.json &
    upload_pids="$upload_pids $!"
    scp -q -i $SSH_KEY_LOC $warmup_cfg_loc $SSH_USERNAME@${MACHINES[0]}:~/warmup_config.json &
    upload_pids="$upload_pids $!"
    wait $upload_pids

    # move the config to the intended location (including client machine)
    move_pids=""
    for HOST in "${MACHINES[@]}"; do
        ssh -i $SSH_KEY_LOC $SSH_USERNAME@$HOST "sudo mv ~/config.json /usr/local/griya/bin/config.json" &
        move_pids="$move_pids $!"
    done
    ssh -i $SSH_KEY_LOC $SSH_USERNAME@$CLIENT_MACHINE "sudo mv ~/config.json /usr/local/griya/bin/config.json" &
    move_pids="$move_pids $!"
    ssh -i $SSH_KEY_LOC $SSH_USERNAME@$CLIENT_MACHINE "sudo mv ~/warmup_config.json /usr/local/griya/bin/warmup_config.json" &
    move_pids="$move_pids $!"
    ssh -i $SSH_KEY_LOC $SSH_USERNAME@${MACHINES[0]} "sudo mv ~/warmup_config.json /usr/local/griya/bin/warmup_config.json" &
    move_pids="$move_pids $!"
    wait $move_pids
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
            -client_type tcp -client_action block -log_level error -log_stdout \
            -sstimeon -config config.json" \
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
    ssh -i $SSH_KEY_LOC $SSH_USERNAME@$CLIENT \
        "cd /usr/local/griya/bin; \
        sudo ./client -id 1.1 -algorithm ${ALG} \
        -client_type tcp -client_action block -log_level info -config warmup_config.json"
    ssh -i $SSH_KEY_LOC $SSH_USERNAME@$CLIENT \
        "cd /usr/local/griya/bin; \
        sudo rm latency encode_time"
    
    echo "> runing the clients and record the latencies ..."
    ssh -i $SSH_KEY_LOC $SSH_USERNAME@$CLIENT \
        "cd /usr/local/griya/bin; \
        sudo ./client -id 1.1 -algorithm ${ALG} \
        -client_type tcp -client_action block -log_level info -config config.json -log_stdout"
}

# $1 is the value size
# $2 is the protocol name (paxos or opaxos)
# $3 is the secret-sharing algorithm (for opaxos)
function gather_results {
    echo "> gathering the latency data ..."
    
    # gather the latency data
    target_loc="results/${crt_time}_latency_bw/latency_$2_$3_$1.dat"
    scp -q -i $SSH_KEY_LOC $SSH_USERNAME@$CLIENT:/usr/local/griya/bin/latency $target_loc

    # gather the secret-sharing latency
    target_loc="results/${crt_time}_latency_bw/encode_$2_$3_$1.dat"
    scp -q -i $SSH_KEY_LOC $SSH_USERNAME@$CLIENT:/usr/local/griya/bin/encode_time $target_loc
}

# kill the instances in all the machines by killing the ssh background process
function kill_instances {
    for pid in "${PIDS[@]}"; do
        kill $pid
    done
    kill_pids=""
    for n in "${MACHINES[@]}"; do
        ssh -i $SSH_KEY_LOC $SSH_USERNAME@$n \
            "sudo killall ./server > /dev/null 2>&1; \
            cd /usr/local/griya/bin; sudo ./cleanup.sh > /dev/null 2>&1; \
            sudo rm -f encode_time latency client.log > /dev/null 2>&1" &
        kill_pids="$kill_pids $!"
    done
    wait $kill_pids
}

prepare_result_dir

# begin the measurements
for E in "${EXPERIMENTS[@]}"; do
    for SZ in "${VAL_SIZES[@]}"; do
        echo "=> Run a measurement (protocol: $E, value size: $SZ bytes)"
        kill_prev_instances
        prepare_config $SZ $E
        run_instances $E
        run_clients $E
        gather_results $SZ $E
        kill_instances
        
        echo ""
        sleep 1
    done
done
