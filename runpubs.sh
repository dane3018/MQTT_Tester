#!/bin/bash

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

# Trap SIGINT (Ctrl+C) to call the function to terminate processes
trap terminate_processes SIGINT

# Loop to run 5 instances of your Python script with argument 'i'
for i in {1..5}
do
    python3 publisher.py $i &
    pids+=($!)  # Store the process ID of each background process
done

# Wait for all instances to finish
wait
