#!/bin/bash 
# script to run the capacity overhead evaluation
# the final results are .csv file containing the avg. latencies and resp. rate with
# increasing load

source variables.sh
crt_time=$(date +"%Y%m%d%H%M%S")

EXPERIMENTS=(
    "paxos"
    "opaxos shamir"
    "opaxos ssms"
)
LOADS=()
NUM_CLIENTS=10
DURATION=30
VAL_SIZE_BYTE=5000
NUM_REPETITION=1

function gen_loads {
    load_start=200
    load_end=100000
    load_inc=200

    LOADS+=( 100 )
    while [ $load_start -le $load_end ]; do
        LOADS+=($load_start)
        load_start=$(($load_start+$load_inc))
    done
}

function kill_prev_instances {
    PRE_CMDS=(
        'sudo killall ./server'                     # kill the previous process, if any, using the process name 'server'
        "sudo fuser -k ${INTERNAL_PORT}/tcp"        # kill the previous process, if any, using the tcp port
        'sudo killall ./client'                     # kill the previous client process, if any, using the process name 'server'
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
    mkdir -p results/${crt_time}_capacity
    echo "runtime(s), algo, label, target_load(req/s), load(req/s), response_rate(req/s), load_diff, avg_lat(ms)" > results/${crt_time}_capacity/capacity.csv
}

# prepare the config file
# $1 is the protocol name (paxos or opaxos)
# $2 is the secret-sharing algorithm (for opaxos)
# $3 the intended load target with all the clients
function prepare_config {
    cfg_loc="results/${crt_time}_capacity/cfg_$1_$2_$3.json"
    warmup_cfg_loc="results/${crt_time}_capacity/cfg_$1_$2_$3_warmup.json"
    cp dist_capacity.template $cfg_loc
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
    for HOST in "${HOSTS[@]}"; do
        if [ $i -eq 1 ]; then
            temp="$temp\"1.$i\": \"proposer,acceptor,learner\", "
        else
            temp="$temp\"1.$i\": \"acceptor,learner\", "
        fi
        i=$((i+1))
    done
    temp=${temp%??}
    sed -i "" "s%X_ROLES%${temp}%g" $cfg_loc

    # insert the protocol info
    case $1 in
        paxos)
            temp="\"name\": \"paxos\"";;
        opaxos)
            case $2 in
                shamir)
                    temp="\"name\": \"opaxos\", \"secret_sharing\": \"shamir\", \"threshold\": 2, \"quorum_1\": 4, \"quorum_2\": 3, \"quorum_fast\": 4";;
                ssms)
                    temp="\"name\": \"opaxos\", \"secret_sharing\": \"ssms\", \"threshold\": 2, \"quorum_1\": 4, \"quorum_2\": 3, \"quorum_fast\": 4";;
            esac
            ;;
    esac
    sed -i "" "s%X_PROTOCOL%${temp}%g" $cfg_loc

    # insert the number of clients
    sed -i "" "s%X_NUM_CLIENTS%${NUM_CLIENTS}%g" $cfg_loc

    # insert the value size
    sed -i "" "s%X_VAL_SIZE%${VAL_SIZE_BYTE}%g" $cfg_loc
    
    # insert the intended load
    load_per_client=$(($3/$NUM_CLIENTS))
    sed -i "" "s%X_LOAD_PER_CLIENT%${load_per_client}%g" $cfg_loc

    # insert the duration time
    sed "s%X_DURATION%1%g" $cfg_loc > $warmup_cfg_loc
    sed -i "" "s%X_DURATION%${DURATION}%g" $cfg_loc

    # upload the config to all the machines
    for HOST in "${MACHINES[@]}"; do
        scp -q -i $SSH_KEY_LOC $cfg_loc $SSH_USERNAME@$HOST:~/config.json
        ssh -i $SSH_KEY_LOC $SSH_USERNAME@$HOST "sudo mv ~/config.json /usr/local/griya/bin/config.json"
    done

    # upload warmup config to the first machine
    scp -q -i $SSH_KEY_LOC $warmup_cfg_loc $SSH_USERNAME@${MACHINES[0]}:~/warmup_config.json
    ssh -i $SSH_KEY_LOC $SSH_USERNAME@${MACHINES[0]} "sudo mv ~/warmup_config.json /usr/local/griya/bin/warmup_config.json"
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
        
        ssh -o ServerAliveInterval=60 -i $SSH_KEY_LOC $SSH_USERNAME@$HOST \
            "cd /usr/local/griya/bin; \
            sudo cset shield -e env -- GOGC=100 ./server -id ${ID} -algorithm ${ALG} \
            -client_type unix -client_action pipeline -log_level error -log_stdout \
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
    ssh -o ServerAliveInterval=60 -i $SSH_KEY_LOC $SSH_USERNAME@${MACHINES[0]} \
        "cd /usr/local/griya/bin; \
        sudo ./client -id 1.1 -algorithm ${ALG} \
        -client_type unix -client_action pipeline -log_level error -config warmup_config.json -log_stdout"
    ssh -i $SSH_KEY_LOC $SSH_USERNAME@${MACHINES[0]} \
        "cd /usr/local/griya/bin; \
        sudo ./cleanup.sh  > /dev/null 2>&1; sudo rm -f latency encode_time"
    
    echo "> runing the clients in the first machine ..."
    ssh -o ServerAliveInterval=60 -i $SSH_KEY_LOC $SSH_USERNAME@${MACHINES[0]} \
        "cd /usr/local/griya/bin; \
        sudo ./client -id 1.1 -algorithm ${ALG} \
        -client_type unix -client_action pipeline -log_level info -config config.json"
}

# $1 is the protocol name (paxos or opaxos)
# $2 is the secret-sharing algorithm (for opaxos)
# $3 the intended load target with all the clients
function gather_results {
    echo "> gathering the data ..."
    
    # gather the latency data
    target_loc="results/${crt_time}_capacity/latency_$1_$2_$3.dat"
    scp -q -i $SSH_KEY_LOC $SSH_USERNAME@${MACHINES[0]}:/usr/local/griya/bin/latency $target_loc

    # rename the output file to client.log
    ssh -i $SSH_KEY_LOC $SSH_USERNAME@${MACHINES[0]} \
        'cd /usr/local/griya/bin; ls -t client.*.log | head -n 1 | sudo xargs -I{} mv {} client.log'

    # gather the cients' log
    target_loc="results/${crt_time}_capacity/clients_$1_$2_$3.log"
    scp -q -i $SSH_KEY_LOC $SSH_USERNAME@${MACHINES[0]}:/usr/local/griya/bin/client.log $target_loc

    # parse the load, throughput, and avg latency from the log
    actual_load=$(grep 'request-rate' ${target_loc} | awk '{sum += $NF} END {print sum}')
    throughput=$(grep 'Throughput' ${target_loc} | awk '{print $NF}')
    load_diff_percent=$(echo "scale=4; ($actual_load-$throughput)/$actual_load*100" | bc )
    avg_lat=$(grep 'mean' ${target_loc} | awk '{print $NF}')
    
    echo "   intended load  : $3 req/s"
    echo "   performed load : $actual_load req/s"
    echo "   response rate  : $throughput req/s"
    echo "   load diff      : $load_diff_percent %"
    echo "   avg latency    : $avg_lat ms"

    echo "$DURATION, $1, $1_$2, $3, $actual_load, $throughput, $load_diff_percent, $avg_lat" >> results/${crt_time}_capacity/capacity.csv
}

# kill the instances in all the machines by killing the ssh background process
function kill_instances {
    for pid in "${PIDS[@]}"; do
        kill $pid
    done
    for n in "${MACHINES[@]}"; do
        ssh -i $SSH_KEY_LOC $SSH_USERNAME@$n \
            "sudo killall ./server > /dev/null 2>&1;" &
    done
    wait
}

function cleanup {
    for n in "${MACHINES[@]}"; do
        ssh -i $SSH_KEY_LOC $SSH_USERNAME@$n \
            "cd /usr/local/griya/bin; sudo ./cleanup.sh > /dev/null 2>&1; \
            sudo rm -f encode_time client.log" &
    done
    sleep 1
}

gen_loads
prepare_result_dir

# begin the measurements
rep_id=1
while [ $NUM_REPETITION -gt 0 ]; do
    for E in "${EXPERIMENTS[@]}"; do
        protocol=$(echo $E | awk '{print $1;}')
        ss_alg=$(echo $E | awk '{print $2;}')
        load_diff_percent=0

        # running all the instances
        kill_prev_instances
        prepare_config "$protocol" "$ss_alg" 0
        run_instances "$protocol"
        
        # vary load sent by the clients
        for LD in "${LOADS[@]}"; do
            echo "=> Run a measurement (repetition#: $rep_id, protocol: $E, load: $LD req/s)"
            prepare_config "$protocol" "$ss_alg" $LD
            sleep 3
            run_clients "$protocol"
            gather_results "$protocol" "$ss_alg" $LD
            cleanup

            echo ""

            # break if the previous load diff is greater 
            # than or equal to 5%, check in gather_results()
            if [[ "$(bc <<< "$load_diff_percent >= 5")" == "1" ]]; then
                break
            fi
        done

        kill_instances

    done

    NUM_REPETITION=$(($NUM_REPETITION-1))
    rep_id=$(($rep_id+1))
done
