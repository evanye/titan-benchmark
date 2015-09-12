#!/bin/bash
set -e

dataset=livejournal
latency=T
throughput=T
QUERY_DIR=/mnt/liveJournal-40attr16each-queries
OUTPUT_DIR=/mnt/livejournal_output

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

for test in "${tests[@]}"
do
  if [[ -n "$latency" ]]; then
    sleep 2 && sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
    mvn exec:java -Dexec.mainClass="edu.berkeley.cs.benchmark.Benchmark" \
      -Dexec.args="${test} latency ${dataset} ${QUERY_DIR} ${OUTPUT_DIR} ${warmup} ${measure}"
  fi

  if [[ -n "$throughput" ]]; then
    sleep 2 && sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
    mvn exec:java -Dexec.mainClass="edu.berkeley.cs.benchmark.Benchmark" \
      -Dexec.args="${test} throughput ${dataset} ${QUERY_DIR} ${OUTPUT_DIR} ${warmup} ${measure}"
  fi
  #java -verbose:gc -Xmx${JVM_HEAP}m -cp ${classpath} \
  #   edu.berkeley.cs.benchmark.BenchTao ${test} \
  #   latency \
  #   ${dataset} \
  #   ${QUERY_DIR} \
  #   ${OUTPUT_DIR} \
  #   ${warmup} \
  #   ${measure} \
done
