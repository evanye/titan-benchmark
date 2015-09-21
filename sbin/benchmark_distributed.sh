#!/usr/bin/env bash
set -e

#### Initial setup

dataset=twitter
sbin=/home/ubuntu/titan-benchmark/sbin
host_file=nsdi-10cass.hosts
HOSTLIST=`cat ${sbin}/../conf/${host_file}`
query_dir=${sbin}/../../twitter2010-40attr16each-queries-with-supernodes
results=~/results

OUTPUT_DIR=output
warmup=60
measure=120
cooldown=45

numClients=(16 32 64)
tests=(
  # Primitive queries
  Neighbor
  NeighborNode
  EdgeAttr
  NeighborAtype
  NodeNode
  MixPrimitive
  # TAO queries
  # AssocRange
  # ObjGet
  # AssocGet
  # AssocCount
  # AssocTimeRange
  MixTao
)

#### Copy the repo files over
for host in `echo "$HOSTLIST"|sed  "s/#.*$//;/^$/d"`; do
  rsync -arL ${sbin}/../ ${host}:titan-benchmark &
  rsync -arL ${sbin}/../../twitter2010-40attr16each-queries-with-supernodes ${host}:~ &
done
wait
echo "Synced benchmark repo and queries to all servers."

bash ${sbin}/hosts.sh \
  mkdir -p ${OUTPUT_DIR}
bash ${sbin}/hosts.sh \
  mvn -f titan-benchmark/pom.xml compile

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

