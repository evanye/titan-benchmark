package edu.berkeley.cs.benchmark;

import edu.berkeley.cs.titan.Graph;

public class AssocCount extends Benchmark {
    public static final String WARMUP_FILE = "assocCount_warmup.txt";
    public static final String QUERY_FILE = "assocCount_query.txt";

    @Override
    public void readQueries() {
        getLongInteger(WARMUP_FILE, warmupAssocCountNodes, warmupAssocCountAtypes);
        getLongInteger(QUERY_FILE, assocCountNodes, assocCountAtypes);
    }

    @Override
    public int warmupQuery(Graph g, int i) {
        g.assocCount(modGet(warmupAssocCountNodes, i), modGet(warmupAssocCountAtypes, i));
        return 1;
    }

    @Override
    public int query(Graph g, int i) {
        g.assocCount(modGet(assocCountNodes, i), modGet(assocCountAtypes, i));
        return 1;
    }

}
