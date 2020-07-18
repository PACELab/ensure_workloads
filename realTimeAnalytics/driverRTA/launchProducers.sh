#! /bin/sh

outputDir=$1
messagesPerSec=$2
numMessages=$3
prefixDir="/home/pace_admin/serverless/serverless-training-set/realTimeAnalytics/driverRTA"
script="$prefixDir"/producerRTA.py

time $script rta_v1 init_msg $messagesPerSec 100 $numMessages 0 1 > $outputDir/produce_partition0.log &
time $script rta_v1 init_msg $messagesPerSec 100 $numMessages 1 1 > $outputDir/produce_partition1.log &
time $script rta_v1 init_msg $messagesPerSec 100 $numMessages 2 1 > $outputDir/produce_partition2.log &
time $script rta_v1 init_msg $messagesPerSec 100 $numMessages 3 1 > $outputDir/produce_partition3.log &
wait

