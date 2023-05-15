#!/bin/bash 
# script to setup latency and bandwidth emulation for the evaluation
# usage     : ./init_networks.sh
# to undo   : ./init_networks.sh reset

source variables.sh

if [ ! -z "$@" ] && [ "$@" == "reset" ]; then
    echo "==> Reset the emulated networks in all machines ..."
    echo "> please ignore error when the qdisc has zero handles"
    for nid in "${!MACHINES[@]}"; do
        src_machine=${MACHINES[$nid]}
        h=${HOSTS[$nid]}
        echo "> reset at machine $h"
        ssh -o StrictHostKeyChecking=no -i $SSH_KEY_LOC $SSH_USERNAME@${src_machine} \
            "sudo tc qdisc del dev ${INTERFACE_NAME} root"
    done
    exit
fi

echo "==> Setup the rate-limited link ..."
for id_src in "${!MACHINES[@]}"; do
    initialized=0
    for id_dst in  "${!MACHINES[@]}"; do
        varnamebw=BW_${id_src}_${id_dst}
        varnamelat=INJECTED_LATENCY_${id_src}_${id_dst}

        # default value: no rate limit, and no delay
        rate_limit=100000000 # 100Gbps
        delay="0ms"

        # read the intended bw and delay from variables.sh
        if [ -n "${!varnamebw}" ]; then
            rate_limit=${!varnamebw}
            echo " >>> rate-limit: ${rate_limit}"
        fi
        if [ -n "${!varnamelat}" ]; then
            delay=${!varnamelat}
            echo " >>> delay: ${delay}"
        fi

        if [ $rate_limit -ne 100000000 ] || [ "$delay" != "0ms" ]; then
            src_machine=${MACHINES[$id_src]}
            dst_host=${HOSTS[$id_dst]}
            dst_minor=$(($id_dst+1))

            echo "    * setup rules for ${HOSTS[${id_src}]} -> ${HOSTS[${id_dst}]} traffic"

            # initialize the root qdisc in the src_machine, need to do this once for each machine
            if [ $initialized -eq 0 ]; then
                echo "      initialize the root qdisc in ${HOSTS[${id_src}]}:"
                echo "        sudo tc qdisc add dev ${INTERFACE_NAME} root handle 1: htb"
                ssh -o StrictHostKeyChecking=no -i $SSH_KEY_LOC $SSH_USERNAME@${src_machine} \
                    "sudo tc qdisc del dev ${INTERFACE_NAME} root"
                ssh -o StrictHostKeyChecking=no -i $SSH_KEY_LOC $SSH_USERNAME@${src_machine} \
                    "sudo tc qdisc add dev ${INTERFACE_NAME} root handle 1: htb"
                initialized=1
            fi

            # add class with bw rate_limit
            echo "     sudo tc class add dev ${INTERFACE_NAME} parent 1: classid 1:${dst_minor} htb rate ${rate_limit}mbit"
            ssh -o StrictHostKeyChecking=no -i $SSH_KEY_LOC $SSH_USERNAME@${src_machine} \
                "sudo tc class add dev ${INTERFACE_NAME} parent 1: classid 1:${dst_minor} htb rate ${rate_limit}mbit"

            # add class with the injected delay
            echo "     sudo tc qdisc add dev ${INTERFACE_NAME} parent 1:${dst_minor} handle ${dst_minor}0: netem delay ${delay}"
            ssh -o StrictHostKeyChecking=no -i $SSH_KEY_LOC $SSH_USERNAME@${src_machine} \
                "sudo tc qdisc add dev ${INTERFACE_NAME} parent 1:${dst_minor} handle ${dst_minor}0: netem delay ${delay}"

            # add a filter mapped to the created class
            echo "     sudo tc filter add dev ${INTERFACE_NAME} protocol ip parent 1: prio 1 u32 match ip dst ${dst_host}/32 flowid 1:${dst_minor}"
            ssh -o StrictHostKeyChecking=no -i $SSH_KEY_LOC $SSH_USERNAME@${src_machine} \
                "sudo tc filter add dev ${INTERFACE_NAME} protocol ip parent 1: prio 1 u32 match ip dst ${dst_host}/32 flowid 1:${dst_minor}"

        fi
    done
done
echo "done."
