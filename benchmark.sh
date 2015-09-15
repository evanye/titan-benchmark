#!/bin/bash
set -e

dataset=livejournal
latency=false
throughput=true
QUERY_DIR=/mnt/liveJournal-40attr16each-queries
OUTPUT_DIR=/mnt/output

# List of all possible queries you can benchmark against
# Comment any out if you don't want to benchmark them
tests=(
  # Primitive queries
  Neighbor
  NeighborNode
  NeighborAtype
  Node
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
export MAVEN_OPTS="-Xmx102400M"

warmup=100000
measure=100000
numClients=( 1 8 64 )

if [ "$latency" = true ]; then
  for test in "${tests[@]}"; do
    sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
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
      sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
      nodetool invalidaterowcache
      nodetool invalidatekeycache
      nodetool invalidatecountercache
      sleep 2
      mvn exec:java -Dexec.mainClass="edu.berkeley.cs.benchmark.Benchmark" \
        -Dexec.args="${test} throughput ${dataset} ${QUERY_DIR} ${OUTPUT_DIR} ${numClient} 0 0"
    done
  done
fi
