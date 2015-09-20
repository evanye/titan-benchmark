package edu.berkeley.cs.benchmark;

import edu.berkeley.cs.titan.Graph;

import java.util.List;

public class EdgeAttr extends Benchmark<List<String>> {
    public static final String WARMUP_FILE = "neighborAtype_warmup_100000.txt";
    public static final String QUERY_FILE = "neighborAtype_query_100000.txt";

    @Override
    public void readQueries() {
        getLongInteger(WARMUP_FILE, warmupEdgeNodeIds, warmupEdgeAtype);
        getLongInteger(QUERY_FILE, edgeNodeId, edgeAtype);
    }

    @Override
    public List<String> warmupQuery(Graph g, int i) {
        return g.getEdgeAttrs(modGet(warmupEdgeNodeIds, i), modGet(warmupEdgeAtype, i));
    }

    @Override
    public List<String> query(Graph g, int i) {
        return g.getEdgeAttrs(modGet(edgeNodeId, i), modGet(edgeAtype, i));
    }

    @Override
    public RunThroughput getThroughputJob(int clientId) {
        return new RunThroughput(clientId) {
            @Override
            public void warmupQuery() {
                int idx = rand.nextInt(warmupEdgeNodeIds.size());
                EdgeAttr.this.warmupQuery(g, idx);
            }

            @Override
            public int query() {
                int idx = rand.nextInt(edgeNodeId.size());
                return EdgeAttr.this.query(g, idx).size();
            }
        };
    }
}
