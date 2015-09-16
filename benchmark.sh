#!/bin/bash
set -e

dataset=livejournal
latency=false
throughput=false
QUERY_DIR=/mnt/liveJournal-40attr16each-queries
OUTPUT_DIR=/mnt/output
mkdir -p $OUTPUT_DIR

# List of all possible queries you can benchmark against
# Comment any out if you don't want to benchmark them
tests=(
  # Primitive queries
  Neighbor
  NeighborNode
  NeighborAtype
  EdgeAttr
  NodeNode
  MixPrimitive
  # TAO queries
  AssocRange
  ObjGet
  AssocGet
  AssocCount
  AssocTimeRange
  MixTao
  # Retired Queries
  # Node
)

#JVM_HEAP=6900
#echo "Setting -Xmx to ${JVM_HEAP}m"
export MAVEN_OPTS="-verbose:gc -server -Xmx50000M"

warmup=20000
measure=40000
numClients=( 1 8 16 64 128 )

if [ "$latency" = true ]; then
  for test in "${tests[@]}"; do
    sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
    sudo sh -c 'service cassandra start'
    nodetool invalidaterowcache
    nodetool invalidatekeycache
    nodetool invalidatecountercache
    sleep 2
    mvn exec:java -Dexec.mainClass="edu.berkeley.cs.benchmark.Benchmark" \
      -Dexec.args="${test} latency ${dataset} ${QUERY_DIR} ${OUTPUT_DIR} 1 ${warmup} ${measure}"
    sudo sh -c 'service cassandra stop'
  done
fi

if [[ "$throughput" = true ]]; then
  for test in "${tests[@]}"; do
    for numClient in "${numClients[@]}"; do
      sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
      sudo sh -c 'service cassandra start'
      nodetool invalidaterowcache
      nodetool invalidatekeycache
      nodetool invalidatecountercache
      sleep 2
      mvn exec:java -Dexec.mainClass="edu.berkeley.cs.benchmark.Benchmark" \
        -Dexec.args="${test} throughput ${dataset} ${QUERY_DIR} ${OUTPUT_DIR} ${numClient} 0 0"
      sudo sh -c 'service cassandra stop'
    done
  done
fi
