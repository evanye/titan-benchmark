#!/usr/bin/env bash
set -e

mkdir -p $1
export MAVEN_OPTS="-server -Xmx50000M"
mvn -f /home/ubuntu/titan-benchmark/pom.xml compile

shuf -n 2000000 $2/assocGet_real_query.txt > $2/assocGet_query.txt
shuf -n 300000 $2/assocGet_real_warmup.txt > $2/assocGet_warmup.txt
