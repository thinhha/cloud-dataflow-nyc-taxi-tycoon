#!/bin/bash

usage="$(basename "$0") [-h] [-m p r] -- utility script to submit dataflow jobs

where:
    -h  show this help text
    -m  main class, defaults to 'com.google.codelabs.dataflow.DollarRides'
    -p  target PubSub topic to write result, defaults to 'visualizer'
    -r  where to execute dataflow job, can be either DataflowRunner or DirectRunner
        defaults to DirectRunner"

usage() { echo "$usage" 1>&2; exit 1; }
[ $# -eq 0 ] && usage

while getopts :m:p:r:h: option
do
    case "${option}"
    in
    m) MAIN_CLASS=${OPTARG:-com.google.codelabs.dataflow.DollarRides};;
    p) MY_TARGET_PUBSUB_ID=${OPTARG:-visualizer};;
    r) RUNNER=${OPTARG:-DirectRunner};;
    *) usage ;;
    esac
done
shift $((OPTIND-1))

MY_PROJECT=skydataflowworkshop
MY_STAGING_BUCKET=tempbucketsky
MY_SOURCE_PUBSUB_ID=taxirides-realtime

if [ "$RUNNER" == "DirectRunner" ]
then
    mvn compile exec:java \
        -Dexec.cleanupDaemonThreads=false\
        -Dexec.mainClass=$MAIN_CLASS \
        -Dexec.args="--project=$MY_PROJECT \
        --sourceProject=pubsub-public-data \
        --sinkProject=$MY_PROJECT \
        --sourceTopic=$MY_SOURCE_PUBSUB_ID \
        --sinkTopic=$MY_TARGET_PUBSUB_ID \
        --stagingLocation=gs://${MY_STAGING_BUCKET}/staging \
        --tempLocation=gs://${MY_STAGING_BUCKET}/tmp \
        --runner=DirectRunner"
elif [ "$RUNNER" == "DataflowRunner" ]
then
    mvn compile exec:java \
        -Dexec.cleanupDaemonThreads=false\
        -Dexec.mainClass=$MAIN_CLASS \
        -Dexec.args="--project=$MY_PROJECT \
        --sourceProject=pubsub-public-data \
        --sinkProject=$MY_PROJECT \
        --sourceTopic=$MY_SOURCE_PUBSUB_ID \
        --sinkTopic=$MY_TARGET_PUBSUB_ID \
        --stagingLocation=gs://${MY_STAGING_BUCKET}/staging \
        --tempLocation=gs://${MY_STAGING_BUCKET}/tmp \
        --runner=DataflowRunner \
        --streaming=true \
        --numWorkers=1 \
        --zone=europe-west1-c"
else
    echo "Invalid Runner" 1>&2; exit 1; 
fi