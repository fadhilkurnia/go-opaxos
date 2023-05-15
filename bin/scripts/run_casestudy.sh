#!/bin/bash 

source variables.sh
crt_time=$(date +"%Y%m%d%H%M%S")

EXPERIMENTS=(
    "fastopaxos shamir"
    "opaxos shamir"
    "paxos"
)
CLIENT_TYPE=( "inhouse" "remote" )
DATASETS=( "pingpong" "moniotr" )

# CLIENT=${MACHINES[0]} # uncomment this if the clients need to be run in the first machine
CLIENT=$CLIENT_MACHINE  # uncomment this to run the clients in a separate machine

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
    mkdir -p results/${crt_time}_latency_casestudy
}

# prepare the config file
# $1 is the client location: 'inhouse' or 'remote'
# $2 is the protocol name (paxos or opaxos or fastopaxos)
# $3 is the secret-sharing algorithm (for opaxos and fastopaxos)
function prepare_config {
    cfg_loc="results/${crt_time}_latency_casestudy/cfg_$1_$2_$3.json"
    cp dist_latency_casestudy_${1}.template $cfg_loc
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
        fastopaxos)
            case $3 in
                shamir)
                    temp="\"name\": \"opaxos\", \"secret_sharing\": \"shamir\", \"threshold\": 2, \"quorum_1\": 4, \"quorum_2\": 3, \"quorum_fast\": 4";;
                ssms)
                    temp="\"name\": \"opaxos\", \"secret_sharing\": \"ssms\", \"threshold\": 2, \"quorum_1\": 4, \"quorum_2\": 3, \"quorum_fast\": 4";;
            esac
            ;;
    esac
    sed -i "" "s%X_PROTOCOL%${temp}%g" $cfg_loc

    # upload the config to all the machines (including client machine)
    upload_pids=""
    for HOST in "${MACHINES[@]}"; do
        scp -q -i $SSH_KEY_LOC $cfg_loc $SSH_USERNAME@$HOST:~/config.json &
        upload_pids="$upload_pids $!"
    done
    scp -q -i $SSH_KEY_LOC $cfg_loc $SSH_USERNAME@$CLIENT_MACHINE:~/config.json &
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
    wait $move_pids
}

# upload tracefile to the machine used for the clients
# $1 is the dataset name ('moniotr' or 'pingpong')
# $2 is the client location ('inhouse' or 'remote')
function upload_tracefiles {
    tracefile_loc="../tracefiles/$1_$2.trace"
    echo "> uploading tracefile ..."
    echo "   trace_loc: $tracefile_loc"

    scp -q -i $SSH_KEY_LOC $tracefile_loc $SSH_USERNAME@$CLIENT:~/$1_$2.trace
    ssh -i $SSH_KEY_LOC $SSH_USERNAME@$CLIENT "sudo mkdir -p /usr/local/griya/bin/tracefiles"
    ssh -i $SSH_KEY_LOC $SSH_USERNAME@$CLIENT "sudo mv ~/$1_$2.trace /usr/local/griya/bin/tracefiles/$1_$2.trace"
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
            -client_type tcp -log_level error -log_stdout \
            -config config.json" \
            &
        
        # store the pid of the ssh background process so we can kill it later
        PIDS+=($!)

        i=$(($i+1))
    done

    # wait until all the instances in all the machines are ready
    sleep 1
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

# run multiple clients in the first machine
# $1 is the dataset name ('moniotr' or 'pingpong')
# $2 is the client location ('inhouse' or 'remote')
# $3 is the protocol name (paxos or opaxos)
function run_clients {
    ALG=$3
    
    echo "> running clients at ${CLIENT}"
    echo "> warming up ..."
    ssh -i $SSH_KEY_LOC $SSH_USERNAME@$CLIENT \
        "cd /usr/local/griya/bin; \
        sudo ./client -id 1.1 -algorithm ${ALG} \
        -client_type tcp -client_action tracefile -trace tracefiles/${1}_${2}.trace \
        -log_level debug -config config.json"
    ssh -i $SSH_KEY_LOC $SSH_USERNAME@$CLIENT \
        "cd /usr/local/griya/bin; \
        sudo rm latency encode_time"
    
    echo "> runing the clients and record the latencies ..."
    ssh -i $SSH_KEY_LOC $SSH_USERNAME@$CLIENT \
        "cd /usr/local/griya/bin; \
        sudo ./client -id 1.1 -algorithm ${ALG} \
        -client_type tcp -client_action tracefile -trace tracefiles/${1}_${2}.trace \
        -log_level info -config config.json -log_stdout"
}

# $1 is the dataset name ('moniotr' or 'pingpong')
# $2 is the client location ('inhouse' or 'remote')
# $3 is the protocol name (paxos or opaxos or fastopaxos)
# $4 is the secret-sharing algorithm (for opaxos or fastopaxos)
function gather_results {
    echo "> gathering the latency data ..."
    
    # gather the latency data
    target_loc="results/${crt_time}_latency_casestudy/latency_$1_$2_$3_$4.dat"
    scp -q -i $SSH_KEY_LOC $SSH_USERNAME@$CLIENT:/usr/local/griya/bin/latency $target_loc
}

# begin the measurement
prepare_result_dir

for D in "${DATASETS[@]}"; do
    for ct in "${CLIENT_TYPE[@]}"; do

        upload_tracefiles $D $ct

        for E in "${EXPERIMENTS[@]}"; do
            echo "=> Run a measurement (dataset: $D, protocol: $E, client-location: $ct)"
            kill_prev_instances
            prepare_config $ct $E
            run_instances $E
            run_clients $D $ct $E
            gather_results $D $ct $E
            kill_instances

            echo ""
            sleep 1
        done
    done
done