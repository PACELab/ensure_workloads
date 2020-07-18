#! /bin/sh


outputDir=$1
numInvocations=$2
launchType=$3
btwLaunchSleep=$4
batchSize=$5
numInstances=1
numReqsPerIter=$6

mkdir -p $outputDir
prefixDir="/home/pace_admin/serverless/serverless-training-set/realTimeAnalytics/driverRTA"
script="$prefixDir"/kafkaWrapper.py
cmd="time  $script realTimeAnalytics_v1 rta_v1 init_msg $numInvocations $batchSize $outputDir 0 $launchType $btwLaunchSleep 1 > $outputDir/launch_0.log &"
echo "cmd --> $cmd "
case $numInstances in
    1)
        echo "1. numInstances: $numInstances"
        time  $script realTimeAnalytics_v1 rta_v1 init_msg $numInvocations $batchSize $outputDir 0 $launchType $btwLaunchSleep $numReqsPerIter 1 > $outputDir/launch_0.log &
        ;;
    2)
        echo "2. numInstances: $numInstances"
        time  $script realTimeAnalytics_v1 rta_v1 init_msg $numInvocations $batchSize $outputDir 0 $launchType $btwLaunchSleep 1 > $outputDir/launch_0.log &
        time  $script realTimeAnalytics_v1 rta_v1 init_msg $numInvocations $batchSize $outputDir 1 $launchType $btwLaunchSleep 1 > $outputDir/launch_1.log &
        ;;        
    3)
        echo "3. numInstances: $numInstances"
        time  $script realTimeAnalytics_v1 rta_v1 init_msg $numInvocations $batchSize $outputDir 0 $launchType $btwLaunchSleep 1 > $outputDir/launch_0.log &
        time  $script realTimeAnalytics_v1 rta_v1 init_msg $numInvocations $batchSize $outputDir 1 $launchType $btwLaunchSleep 1 > $outputDir/launch_1.log &
        time  $script realTimeAnalytics_v1 rta_v1 init_msg $numInvocations $batchSize $outputDir 2 $launchType $btwLaunchSleep 1 > $outputDir/launch_2.log &
        ;;        
    4)
        echo "4. numInstances: $numInstances"
        time  $script realTimeAnalytics_v1 rta_v1 init_msg $numInvocations $batchSize $outputDir 0 $launchType $btwLaunchSleep 1 > $outputDir/launch_0.log &
        time  $script realTimeAnalytics_v1 rta_v1 init_msg $numInvocations $batchSize $outputDir 1 $launchType $btwLaunchSleep 1 > $outputDir/launch_1.log &
        time  $script realTimeAnalytics_v1 rta_v1 init_msg $numInvocations $batchSize $outputDir 2 $launchType $btwLaunchSleep 1 > $outputDir/launch_2.log &
        time  $script realTimeAnalytics_v1 rta_v1 init_msg $numInvocations $batchSize $outputDir 3 $launchType $btwLaunchSleep 1 > $outputDir/launch_3.log &
        ;;
    *)
        echo "Undefined numInstances: $numInstances"        
        exit
        ;;
esac
wait
