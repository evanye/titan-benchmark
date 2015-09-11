#!/bin/bash
set -e

dataset=higgs
latencyOrThroughput=throughput
QUERY_DIR=/mnt/liveJournal-40attr16each-queries
OUTPUT_DIR=/mnt/higgs_output


tests=(
  Neighbor
  NeighborNode
  NeighborAtype
  Node
  NodeNode
  Mix
)


#benchAssocRange=T
#benchObjGet=T
#benchAssocGet=T
#benchAssocCount=T
#benchAssocTimeRange=T
#benchTAOMix=T

#JVM_HEAP=6900
#echo "Setting -Xmx to ${JVM_HEAP}m"
export MAVEN_OPTS="-Xmx102400M"

warmup=100000
measure=100000

for test in "${tests[@]}"
  sleep 2 && sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
  mvn exec:java -Dexec.mainClass="edu.berkeley.cs.benchmark.Benchmark" \
    -Dexec.args="${test} ${latencyOrThroughput} ${dataset} ${QUERY_DIR} ${OUTPUT_DIR} ${warmup} ${measure}"
  #java -verbose:gc -Xmx${JVM_HEAP}m -cp ${classpath} \
  #   edu.berkeley.cs.benchmark.Benchmark ${test} \
  #   latency \
  #   ${dataset} \
  #   ${QUERY_DIR} \
  #   ${OUTPUT_DIR} \
  #   ${warmup} \
  #   ${measure} \
done
