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
        return g.getNeighbors(warmupNeighborIds[i]);
    }

    @Override
    public List<Long> query(Graph g, int i) {
        return g.getNeighbors(neighborIds[i]);
    }

    @Override
    public RunThroughput getThroughputJob(int clientId) {
        return new RunThroughput(clientId) {
            @Override
            public void warmupQuery() {
                int idx = rand.nextInt(neighbor_warmup);
                Neighbor.this.warmupQuery(g, idx);
            }

            @Override
            public int query() {
                int idx = rand.nextInt(neighbor_query);
                return Neighbor.this.query(g, idx).size();
            }
        };
    }
}
