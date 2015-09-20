package edu.berkeley.cs.benchmark;

import edu.berkeley.cs.titan.Graph;

import java.util.List;

public class NeighborAtype extends Benchmark<List<Long>> {
    public static final String WARMUP_FILE = "neighborAtype_warmup_100000.txt";
    public static final String QUERY_FILE = "neighborAtype_query_100000.txt";

    @Override
    public void readQueries() {
        getLongInteger(WARMUP_FILE, warmupNeighborAtypeIds, warmupNeighborAtype);
        getLongInteger(QUERY_FILE, neighborAtypeIds, neighborAtype);
    }

    @Override
    public List<Long> warmupQuery(Graph g, int i) {
        return g.getNeighborAtype(modGet(warmupNeighborAtypeIds, i), modGet(warmupNeighborAtype, i));
    }

    @Override
    public List<Long> query(Graph g, int i) {
        return g.getNeighborAtype(modGet(neighborAtypeIds, i), modGet(neighborAtype, i));
    }

    @Override
    public RunThroughput getThroughputJob(int clientId) {
        return new RunThroughput(clientId) {
            @Override
            public void warmupQuery() {
                NeighborAtype.this.warmupQuery(g, rand.nextInt(warmupNeighborAtypeIds.size()));
            }

            @Override
            public int query() {
                int idx = rand.nextInt(neighborAtypeIds.size());
                return NeighborAtype.this.query(g, idx).size();
            }
        };
    }
}
