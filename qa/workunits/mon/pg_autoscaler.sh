#!/bin/bash -ex

NUM_OSDS=$(ceph osd ls | wc -l)
if [ $NUM_OSDS -lt 6 ]; then
    echo "test requires at least 6 OSDs"
    exit 1
fi

NUM_POOLS=$(ceph osd pool ls | wc -l)
if [ $NUM_POOLS -gt 0 ]; then
    echo "test requires no preexisting pools"
    exit 1
fi

function wait_for() {
    local sec=$1
    local cmd=$2

    while true ; do
	if bash -c "$cmd" ; then
	    break
	fi
	sec=$(( $sec - 1 ))
	if [ $sec -eq 0 ]; then
	    echo failed
	    return 1
	fi
	sleep 1
    done
    return 0
}

function power2() { echo "x=l($1)/l(2); scale=0; 2^((x+0.5)/1)" | bc -l;}

# enable
ceph config set mgr mgr/pg_autoscaler/sleep_interval 5
ceph mgr module enable pg_autoscaler

# pg_num_min
ceph osd pool create a 16 --pg-num-min 4
ceph osd pool create b 16 --pg-num-min 2
ceph osd pool set a pg_autoscale_mode on
ceph osd pool set b pg_autoscale_mode on

# get num pools again since we created more pools
NUM_POOLS=$(ceph osd pool ls | wc -l)

# get pool size
POOL_SIZE_A=$(ceph osd pool get a size| grep -Eo '[0-9]{1,4}')
POOL_SIZE_B=$(ceph osd pool get b size| grep -Eo '[0-9]{1,4}')

# calculate target pg of each pools
TARGET_PG_A=$(power2 $((($NUM_OSDS * 100)/($NUM_POOLS)/($POOL_SIZE_A))))
TARGET_PG_B=$(power2 $((($NUM_OSDS * 100)/($NUM_POOLS)/($POOL_SIZE_B))))

# evaluate target_pg against pg num of each pools
wait_for 120 "ceph osd pool get a pg_num | grep $TARGET_PG_A"
wait_for 120 "ceph osd pool get b pg_num | grep $TARGET_PG_B"

# target ratio
ceph osd pool set a target_size_ratio 5
ceph osd pool set b target_size_ratio 1
sleep 10
APGS=$(ceph osd dump -f json-pretty | jq '.pools[0].pg_num_target')
BPGS=$(ceph osd dump -f json-pretty | jq '.pools[1].pg_num_target')
test $APGS -gt 100
test $BPGS -gt 10

# small ratio change does not change pg_num
ceph osd pool set a target_size_ratio 7
ceph osd pool set b target_size_ratio 2
sleep 10
APGS2=$(ceph osd dump -f json-pretty | jq '.pools[0].pg_num_target')
BPGS2=$(ceph osd dump -f json-pretty | jq '.pools[1].pg_num_target')
test $APGS -eq $APGS2
test $BPGS -eq $BPGS2

# target_size
ceph osd pool set a target_size_bytes 1000000000000000
ceph osd pool set b target_size_bytes 1000000000000000
ceph osd pool set a target_size_ratio 0
ceph osd pool set b target_size_ratio 0
wait_for 60 "ceph health detail | grep POOL_TARGET_SIZE_BYTES_OVERCOMMITTED"

ceph osd pool set a target_size_bytes 1000
ceph osd pool set b target_size_bytes 1000
ceph osd pool set a target_size_ratio 1
wait_for 60 "ceph health detail | grep POOL_HAS_TARGET_SIZE_BYTES_AND_RATIO"

ceph osd pool rm a a --yes-i-really-really-mean-it
ceph osd pool rm b b --yes-i-really-really-mean-it

echo OK
