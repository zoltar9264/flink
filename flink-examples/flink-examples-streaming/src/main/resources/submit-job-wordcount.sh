#!/usr/bin/env bash
export HADOOP_CLASSPATH=`hadoop classpath --glob`

CUR_DIR=`pwd`
JAR_NAME="AsyncWordCount.jar"
JAR_PATH="${CUR_DIR}/examples/streaming/${JAR_NAME}"
CLASS_NAME="org.apache.flink.streaming.examples.wordcountasync.AsyncWordCount"
jobName=$1
wordNumber=300000000
wordRate=1000000
wordLength=16
sharingGroup=false
stateMode="mixed"

export JOB_NAME=$jobName
envsubst < ./test-flink-conf-wordcount.yaml > ./conf/flink-conf.yaml

flink_cmd="./bin/flink run"
benchmark_params="-c ${CLASS_NAME} $JAR_PATH -jobName $jobName -wordNumber $wordNumber \
-wordRate $wordRate -wordLength $wordLength -sharingGroup $sharingGroup "
#-checkpointInterval $checkpointInterval

$flink_cmd -d -t yarn-per-job -Dyarn.application.name=$jobName -ys 1 $benchmark_params

