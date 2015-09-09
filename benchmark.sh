#!/bin/bash
set -e

dataset=higgs
QUERY_DIR=/mnt/liveJournal-40attr16each-queries
OUTPUT_DIR=/mnt/higgs_output
classpath=target/scala-2.10/succinctgraph-assembly-0.1.0-SNAPSHOT.jar

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

JVM_HEAP=6900
echo "Setting -Xmx to ${JVM_HEAP}m"

warmup=100000
measure=100000

for test in "${tests[@]}"
  sleep 2 && sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
  java -verbose:gc -Xmx${JVM_HEAP}m -cp ${classpath} \
     edu.berkeley.cs.benchmark.Benchmark ${test} \
     latency \
     ${dataset} \
     ${QUERY_DIR} \
     ${OUTPUT_DIR} \
     ${warmup} \
     ${measure} \
done
