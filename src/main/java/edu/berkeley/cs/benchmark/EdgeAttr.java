package edu.berkeley.cs.benchmark;

import edu.berkeley.cs.titan.Graph;

public class EdgeAttr extends Benchmark {
    public static final String WARMUP_FILE = "neighborAtype_warmup_100000.txt";
    public static final String QUERY_FILE = "neighborAtype_query_100000.txt";

    @Override
    public void readQueries() {
        getLongInteger(WARMUP_FILE, warmupEdgeNodeIds, warmupEdgeAtype);
        getLongInteger(QUERY_FILE, edgeNodeId, edgeAtype);
    }

    @Override
    public int warmupQuery(Graph g, int i) {
        return g.getEdgeAttrs(modGet(warmupEdgeNodeIds, i), modGet(warmupEdgeAtype, i)).size();
    }

    @Override
    public int query(Graph g, int i) {
        return g.getEdgeAttrs(modGet(edgeNodeId, i), modGet(edgeAtype, i)).size();
    }
}
