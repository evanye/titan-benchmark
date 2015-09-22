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
        return g.getNeighborAtype(warmupNeighborAtypeIds[i], warmupNeighborAtype[i]);
    }

    @Override
    public List<Long> query(Graph g, int i) {
        return g.getNeighborAtype(neighborAtypeIds[i], neighborAtype[i]);
    }

    @Override
    public RunThroughput getThroughputJob(int clientId) {
        return new RunThroughput(clientId) {
            @Override
            public void warmupQuery() {
                int idx = rand.nextInt(neighborAtype_warmup);
                NeighborAtype.this.warmupQuery(g, idx);
            }

            @Override
            public int query() {
                int idx = rand.nextInt(neighborAtype_query);
                return NeighborAtype.this.query(g, idx).size();
            }
        };
    }
}
