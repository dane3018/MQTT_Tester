# MQTT Tester

## Run the program 
First ensure that paho client version 1.X for python is installed. If not install using ...

    pip3 install "paho-mqtt<2.0.0"

Also install Pandas 

    pip3 install pandas

Open 2 terminal windows. On the first enter 

    ./runpubs.sh <instancecount> <host>

This will run 'instancecount' publishers that will connect to 'host' and port 1883. In the second terminal run the the following script to start the 180 tests faciliatated by the analyser. 

    python3 analyser.py [host] [port]

Host and port are optional and are set to localhost and 1883 by default.

## Program output 
The analyser will output 6 csv files if all tests are successfully completed. After each analyser qos level test is completed it will create 2 new csv files, output-qos and raw-qos where qos is the analyser's qos for that set of 60 tests. This was done so that if the program does not successfully complete, there will still be some output, while maintaining performance. The output csvs will include the stats required by the assignment as well as some broker stats. The raw csv will contain raw stats message count, max message value and current message value for each topic. 

If the program is interrupted or an exception is thrown, intermediate results will be published to output--1.csv and raw--1.csv.

## Interpretations 
- The analyser will run for 60 seconds directly after receieving the first response message from the publishers. 
- 