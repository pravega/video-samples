#!/bin/bash
set -x
BKB=/opt/bookkeeper/bookkeeper-benchmark/bin/benchmark
NO_EXEC=0
zookeeper=10.200.39.13
quorum=3
ack_quorum=2
ledger=1
run_time=30
result_dir="./results/"
pod_name="benchmark-bookkeeper"
kube="kubectl exec -it ${pod_name}-1 --"
#kube=""
function execute {
    NOW=`date "+%F %T"`
    echo "${NOW} $1"
    if [ ${NO_EXEC} = 0 ] ; then
        eval $1
    fi
}

#declare -a rate0=( 500  1000 2500 5000  10000 15000 17500 20000 )
declare -a rate0=( 500 )
#declare -a rate1=( 1000 2000 5000 10000 15000 20000 )
#declare -a rate2=( 1000 1500 2000 5000  10000 15000 )
#declare -a rate3=( 150  200  250 )
#event_sizes=( 100 1000 10000 1000000 )
event_sizes=( 10000 )
i=0
for size in ${event_sizes[@]}; do
    printf -v s "%08d" $size

    nrates=$((${#rate0[@]}-1))
    for j in $(seq 0 $nrates);  do
        var="rate$i[$j]"
        rate=${!var}
        if [ ! -z ${rate} ] ; then
            printf -v r "%05d" $rate
            execute "${kube} ${BKB} writes -zookeeper ${zookeeper}:2181 -quorum ${quorum} -ackQuorum ${ack_quorum} -entrysize ${size} -ledgers ${ledger} -throttle ${rate} -skipwarmup true -time ${run_time} 2>&1 > ${result_dir}result.BKB1.P1-s${s}-r${r}.log"
        fi
    done
    let i=$i+1
    echo ""
done
echo "Done!"
