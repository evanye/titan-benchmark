package edu.berkeley.cs.benchmark.tao;

public class AssocCount extends BenchTao {
    @Override
    public void readQueries() {
        readAssocCountQueries(queryPath + "/assocCount_warmup.txt",
                warmupAssocCountNodes, warmupAssocCountAtypes);
        readAssocCountQueries(queryPath + "/assocCount_query.txt",
                assocCountNodes, assocCountAtypes);
    }

    @Override
    public int warmupQuery(int i) {
        g.assocCount(modGet(warmupAssocCountNodes, i), modGet(warmupAssocCountAtypes, i));
        return 1;
    }

    @Override
    public int query(int i) {
        long count = g.assocCount(modGet(assocCountNodes, i), modGet(assocCountAtypes, i));
        return 1;
    }

}
