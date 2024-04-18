#!/usr/bin/env bash

BASEDIR=$(dirname "$0")
cd "$BASEDIR" || exit

FLINK_HOME=${BASEDIR}

export HADOOP_CLASSPATH=$(hadoop classpath --glob)
export HDFS_BASE_PATH="hdfs://"`hostname -A | xargs`":9000/"


JAR_NAME="RegularJoinJob.jar"
JAR_PATH="${BASEDIR}/examples/streaming/${JAR_NAME}"

CLASS_NAME="org.apache.flink.streaming.examples.regularjoin.RegularJoinJob"

wordNumber=300000000
wordRate=1000000
wordLength=16
wordMaxCount=-1

sharingGroup=false
flatMapParallelism=1


function _submitJob(){

    envsubst < ${BASEDIR}/test-flink-conf-regularjoin.yaml > ${FLINK_HOME}/conf/flink-conf.yaml

    benchmark_params="-c ${CLASS_NAME} $JAR_PATH -jobName $jobName -joinImpl $joinImpl -wordNumber \
        $wordNumber -wordRate $wordRate -wordLength $wordLength -wordMaxCount $wordMaxCount \
        -wordDistribution $wordDistribution -wordGaussianSigma $wordGaussianSigma -sharingGroup \
        $sharingGroup -flatMapParallelism $flatMapParallelism"

    $FLINK_HOME/bin/flink run -d -t yarn-per-job -Dyarn.application.name=$jobName -ys 1 $benchmark_params
}

function submitJob01(){
    jobName=test-join-sync-random
    export JOB_NAME=$jobName

    joinImpl=SYNC_SIMPLE_JOIN
    wordDistribution=RANDOM

    _submitJob
}

function submitJob02(){
    jobName=test-join-async-random
    export JOB_NAME=$jobName

    joinImpl=ASYNC_SIMPLE_JOIN
    wordDistribution=RANDOM

    _submitJob
}

function submitJob03(){
    jobName=test-join-sync-gaussian_0.2
    export JOB_NAME=$jobName

    joinImpl=SYNC_SIMPLE_JOIN
    wordDistribution=GAUSSIAN
    wordGaussianSigma=0.2

    _submitJob
}

function submitJob04(){
    jobName=test-join-async-gaussian_0.2
    export JOB_NAME=$jobName

    joinImpl=ASYNC_SIMPLE_JOIN
    wordDistribution=GAUSSIAN
    wordGaussianSigma=0.2

    _submitJob
}

function submitJob05(){
    jobName=test-join-sync-gaussian_0.05
    export JOB_NAME=$jobName

    joinImpl=SYNC_SIMPLE_JOIN
    wordDistribution=GAUSSIAN
    wordGaussianSigma=0.05

    _submitJob
}

function submitJob06(){
    jobName=test-join-async-gaussian_0.05
    export JOB_NAME=$jobName

    joinImpl=ASYNC_SIMPLE_JOIN
    wordDistribution=GAUSSIAN
    wordGaussianSigma=0.05

    _submitJob
}

if [[ $# != 1 ]]; then
    echo "Must have and only can have one param indicate job number !"
    exit
fi

case $1 in
    "1")
        submitJob01
        ;;
    "2")
        submitJob02
        ;;
    "3")
        submitJob03
        ;;
    "4")
        submitJob04
        ;;
    "5")
        submitJob05
        ;;
    "6")
        submitJob06
        ;;
    "*")
        echo "submit job 1~6"
        ;;
esac
