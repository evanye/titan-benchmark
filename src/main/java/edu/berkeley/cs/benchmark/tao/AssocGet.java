package edu.berkeley.cs.benchmark.tao;

public class AssocGet extends BenchTao {
    @Override
    public void readQueries() {
        readAssocGetQueries(queryPath + "/assocGet_warmup.txt",
                warmupAssocGetNodes, warmupAssocGetAtypes,
                warmupAssocGetDstIdSets, warmupAssocGetTimeLows,
                warmupAssocGetTimeHighs);

        readAssocGetQueries(queryPath + "/assocGet_query.txt",
                assocGetNodes, assocGetAtypes,
                assocGetDstIdSets, assocGetTimeLows, assocGetTimeHighs);
    }

    @Override
    public int warmupQuery(int i) {
        return g.assocGet(
                modGet(assocGetNodes, i),
                modGet(assocGetAtypes, i),
                modGet(assocGetDstIdSets, i),
                modGet(assocGetTimeLows, i),
                modGet(assocGetTimeHighs, i)).size();
    }

    @Override
    public int query(int i) {
        return g.assocGet(
                modGet(assocGetNodes, i),
                modGet(assocGetAtypes, i),
                modGet(assocGetDstIdSets, i),
                modGet(assocGetTimeLows, i),
                modGet(assocGetTimeHighs, i)).size();
    }
}
