#!/usr/bin/env bash

PID_FILE=server.pid

./server -log_dir=. -log_level=info -id $1 -algorithm $2 -fz=0 -config config-paxos.json &
echo $! >> ${PID_FILE}
