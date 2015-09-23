#!/usr/bin/env bash
set -e

#### Initial setup

dataset=twitter
sbin=/home/ubuntu/titan-benchmark/sbin
host_file=nsdi-10cass.hosts
query_name=twitter2010-40attr16each-queries-no-supernodes-perAtypeCap-10000

results=~/results
OUTPUT_DIR=output

warmup=$((3*60))
measure=$((15*60))
cooldown=$((3*60))

numClients=(16 32 64)
tests=(
  # Primitive queries
  Neighbor
  # NeighborNode
  # EdgeAttr
  # NeighborAtype
  # NodeNode
  MixPrimitive
  # TAO queries
  # AssocRange
  # ObjGet
  # AssocGet
  # AssocCount
  # AssocTimeRange
  # MixTao
)

HOSTLIST=`cat ${sbin}/../conf/${host_file}`
query_dir=${sbin}/../../${query_name}

#### Copy the repo files over
for host in `echo "$HOSTLIST"|sed  "s/#.*$//;/^$/d"`; do
  rsync -arL ${sbin}/../ ${host}:titan-benchmark &
  rsync -arL ${query_dir} ${host}:~ &
done
wait
echo "Synced benchmark repo and queries to all servers."

bash ${sbin}/hosts.sh \
  source ${sbin}/prepare.sh ${OUTPUT_DIR} ${query_dir}

function restart_all() {
  bash ${sbin}/hosts.sh \
    bash ${sbin}/restart_cassandra.sh
  sleep 60
}

function timestamp() {
  date +"%D-%T"
}

for clients in ${numClients[*]}; do
    for test in "${tests[@]}"; do
      restart_all
      bash ${sbin}/hosts.sh \
        mvn -f titan-benchmark/pom.xml exec:java -Dexec.mainClass="edu.berkeley.cs.benchmark.Benchmark" \
          -Dexec.args="${test} throughput ${dataset} ${query_dir} ${OUTPUT_DIR} ${clients} ${warmup} ${measure} ${cooldown}"

      bash ${sbin}/hosts.sh \
        tail -n1 ${OUTPUT_DIR}/${test}_throughput.csv | cut -d'	' -f2 >> ${results}/thput
      sum=$(awk '{ sum += $1} END {print sum}' ~/results/thput)
      cat ${results}/thput

      f="${results}/${test}-${clients}clients.txt"
      t=$(timestamp)
      touch ${f}
      echo "$t,$test" >> ${f}
      cat ${results}/thput >> ${f}

      entry="${t}, ${test}, ${clients}, ${sum}"
      echo $entry
      echo $entry >> ${results}/overall_throughput
      rm ${results}/thput
    done
done

