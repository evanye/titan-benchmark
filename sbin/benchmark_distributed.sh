#!/usr/bin/env bash
set -e

#### Initial setup

dataset=twitter
sbin="`dirname "$0"`"
host_file=nsdi-10cass.hosts
HOSTLIST=`cat ${sbin}/../conf/${host_file}`
query_dir=${sbin}/../../twitter2010-40attr16each-queries-with-supernodes

OUTPUT_DIR=output
warmup=60
measure=60
cooldown=30

numClients=(1)
tests=(
  # Primitive queries
  Neighbor
  # NeighborNode
  # EdgeAttr
  # NeighborAtype
  # NodeNode
  # MixPrimitive
  # TAO queries
  # AssocRange
  # ObjGet
  # AssocGet
  # AssocCount
  # AssocTimeRange
  # MixTao
)

#### Copy the repo files over
for host in `echo "$HOSTLIST"|sed  "s/#.*$//;/^$/d"`; do
  rsync -arL ${sbin}/../ ${host}:titan-benchmark &
  rsync -arL ${sbin}/../../twitter2010-40attr16each-queries-with-supernodes ${host}:queries &
done
wait
echo "Synced benchmark repo and queries to all servers."

function restart_all() {
  bash ${sbin}/hosts.sh \
    bash ${sbin}/restart_cassandra.sh
}

function timestamp() {
  date +"%D-%T"
}

for clients in ${numClients[*]}; do
    for test in "${tests[@]}"; do
      restart_all
      sleep 10
      bash ${sbin}/hosts.sh \
        mkdir -p ${OUTPUT_DIR} \
        mvn exec:java -Dexec.mainClass="edu.berkeley.cs.benchmark.Benchmark" \
          -Dexec.args="${test} throughput ${dataset} ${query_dir} ${OUTPUT_DIR} ${clients} ${warmup} ${measure} ${cooldown}"

      bash ${sbin}/hosts.sh \
        tail -n1 ${OUTPUT_DIR}/${test}_throughput.csv | cut -d'\t' -f2 >> thput
      sum=$(awk '{ sum += $1} END {print sum}' thput)
      cat thput

      f="${test}-${clients}clients.txt"
      t=$(timestamp)
      echo "$t,$test" >> ${f}
      cat thput >> ${f}

      entry="${t}, ${test}, ${clients}, ${sum}"
      cat $entry
      echo $entry >> thput-summary
    done
done

