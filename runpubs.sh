#!/bin/bash

# Chat-GPT was used to create this script

# Array to store process IDs
declare -a pids=()

# Function to terminate all processes
function terminate_processes {
    echo "Terminating all processes..."
    for pid in "${pids[@]}"
    do
        kill $pid
    done
    exit 0
}
if [ $# -lt 2 ]; then
  echo "Usage: $0 <instancecount> <host>"
  exit 1
fi

host=$2
instancecount=$1
# Trap SIGINT (Ctrl+C) to call the function to terminate processes
trap terminate_processes SIGINT

for ((i = 1; i <=instancecount; i++))
do
    python3 publisher.py $i $host &
    pids+=($!)  # Store the process ID of each background process
done

# Wait for all instances to finish
wait
