package edu.berkeley.cs.benchmark;

import edu.berkeley.cs.titan.Graph;

import java.util.List;

public class Neighbor extends Benchmark<List<Long>> {
    public static final String WARMUP_FILE = "neighbor_warmup_100000.txt";
    public static final String QUERY_FILE = "neighbor_query_100000.txt";

    @Override
    public void readQueries() {
        getLong(WARMUP_FILE, warmupNeighborIds);
        getLong(QUERY_FILE, neighborIds);
    }

    @Override
    public List<Long> warmupQuery(Graph g, int i) {
        return g.getNeighbors(modGet(warmupNeighborIds, i));
    }

    @Override
    public List<Long> query(Graph g, int i) {
        return g.getNeighbors(modGet(neighborIds, i));
    }

    @Override
    public RunThroughput getThroughputJob(int clientId) {
        return new RunThroughput(clientId) {
            @Override
            public void warmupQuery() {
                int idx = rand.nextInt(warmupNeighborIds.size());
                g.getNeighbors(modGet(warmupNeighborIds, idx));
            }

            @Override
            public int query() {
                int idx = rand.nextInt(neighborIds.size());
                return g.getNeighbors(modGet(neighborIds, idx)).size();
            }
        };
    }
}
