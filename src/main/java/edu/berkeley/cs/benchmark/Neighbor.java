package edu.berkeley.cs.benchmark;

import edu.berkeley.cs.titan.Graph;

public class Neighbor extends Benchmark {
    public static final String WARMUP_FILE = "neighbor_warmup_100000.txt";
    public static final String QUERY_FILE = "neighbor_query_100000.txt";

    @Override
    public void readQueries() {
        getLong(WARMUP_FILE, warmupNeighborIds);
        getLong(QUERY_FILE, neighborIds);
    }

    @Override
    public int warmupQuery(Graph g, int i) {
        return g.getNeighbors(modGet(warmupNeighborIds, i)).size();
    }

    @Override
    public int query(Graph g, int i) {
        return g.getNeighbors(modGet(neighborIds, i)).size();
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
