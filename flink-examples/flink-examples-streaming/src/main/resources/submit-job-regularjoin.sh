#!/usr/bin/env bash
export HADOOP_CLASSPATH=`hadoop classpath --glob`

CUR_DIR=`pwd`
JAR_NAME="RegularJoinJob.jar"
JAR_PATH="${CUR_DIR}/examples/streaming/${JAR_NAME}"

CLASS_NAME="org.apache.flink.streaming.examples.regularjoin.RegularJoinJob"

wordNumber=300000000
wordRate=1000000
wordLength=16
wordMaxCount=-1

sharingGroup=false
flatMapParallelism=1


function _submitJob(){

    envsubst < ./test-flink-conf-regularjoin.yaml > ./conf/flink-conf.yaml

    flink_cmd="./bin/flink run"
    benchmark_params="-c ${CLASS_NAME} $JAR_PATH -jobName $jobName -joinImpl $joinImpl -wordNumber \
        $wordNumber -wordRate $wordRate -wordLength $wordLength -wordMaxCount $wordMaxCount \
        -wordDistribution $wordDistribution -wordGaussianSigma $wordGaussianSigma -sharingGroup \
        $sharingGroup -flatMapParallelism $flatMapParallelism"

    $flink_cmd -d -t yarn-per-job -Dyarn.application.name=$jobName -ys 1 $benchmark_params
}

function submitJob01(){
    jobName=test-join-sync-random-rsk
    export JOB_NAME=$jobName

    joinImpl=SYNC_SIMPLE_JOIN
    wordDistribution=RANDOM

    _submitJob
}

function submitJob02(){
    jobName=test-join-async-random-rsk
    export JOB_NAME=$jobName

    joinImpl=ASYNC_SIMPLE_JOIN
    wordDistribution=RANDOM

    _submitJob
}

function submitJob03(){
    jobName=test-join-sync-gaussian_0.2-rsk
    export JOB_NAME=$jobName

    joinImpl=SYNC_SIMPLE_JOIN
    wordDistribution=GAUSSIAN
    wordGaussianSigma=0.2

    _submitJob
}

function submitJob04(){
    jobName=test-join-async-gaussian_0.2-rsk
    export JOB_NAME=$jobName

    joinImpl=ASYNC_SIMPLE_JOIN
    wordDistribution=GAUSSIAN
    wordGaussianSigma=0.2

    _submitJob
}

function submitJob05(){
    jobName=test-join-sync-gaussian_0.05-rsk
    export JOB_NAME=$jobName

    joinImpl=SYNC_SIMPLE_JOIN
    wordDistribution=GAUSSIAN
    wordGaussianSigma=0.05

    _submitJob
}

function submitJob06(){
    jobName=test-join-async-gaussian_0.05-rsk
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
