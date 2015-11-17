#!/usr/bin/env bash
sudo sh -c 'service cassandra stop'
sleep 10
sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
sudo sh -c 'service cassandra start'
sleep 20
nodetool enablethrift
nodetool invalidaterowcache
nodetool invalidatekeycache
nodetool invalidatecountercache
sleep 10
