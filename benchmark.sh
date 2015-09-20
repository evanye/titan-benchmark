#!/bin/bash
set -e

dataset=twitter
latency=true
throughput=false
QUERY_DIR=/mnt/mnt/twitter2010-40attr16each-queries
OUTPUT_DIR=/mnt/mnt/output
mkdir -p $OUTPUT_DIR

# List of all possible queries you can benchmark against
# Comment any out if you don't want to benchmark them
tests=(
  # Primitive queries
  Neighbor
  NeighborNode
  EdgeAttr
  NeighborAtype
  # Node
  NodeNode
  MixPrimitive
  # TAO queries
  AssocRange
  ObjGet
  AssocGet
  AssocCount
  AssocTimeRange
  MixTao
)

#JVM_HEAP=6900
#echo "Setting -Xmx to ${JVM_HEAP}m"
export MAVEN_OPTS="-verbose:gc -server -Xmx50000M"

warmup=100000
measure=200000
numClients=( 1 8 64 128 )

if [ "$latency" = true ]; then
  for test in "${tests[@]}"; do
    sudo sh -c 'service cassandra stop'
    sleep 5
    sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
    sudo sh -c 'service cassandra start'
    sleep 15
    nodetool invalidaterowcache
    nodetool invalidatekeycache
    nodetool invalidatecountercache
    sleep 2
    mvn exec:java -Dexec.mainClass="edu.berkeley.cs.benchmark.Benchmark" \
      -Dexec.args="${test} latency ${dataset} ${QUERY_DIR} ${OUTPUT_DIR} 1 ${warmup} ${measure}"
  done
fi

if [[ "$throughput" = true ]]; then
  for test in "${tests[@]}"; do
    for numClient in "${numClients[@]}"; do
      sudo sh -c 'service cassandra stop'
      sleep 5
      sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
      sudo sh -c 'service cassandra start'
      sleep 15
      nodetool invalidaterowcache
      nodetool invalidatekeycache
      nodetool invalidatecountercache
      sleep 2
      mvn exec:java -Dexec.mainClass="edu.berkeley.cs.benchmark.Benchmark" \
        -Dexec.args="${test} throughput ${dataset} ${QUERY_DIR} ${OUTPUT_DIR} ${numClient} 0 0"
    done
  done
fi
