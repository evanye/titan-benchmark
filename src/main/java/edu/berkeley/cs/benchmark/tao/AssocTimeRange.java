package edu.berkeley.cs.benchmark.tao;

public class AssocTimeRange extends BenchTao {
    @Override
    public void readQueries() {
        readAssocTimeRangeQueries(queryPath + "/assocTimeRange_warmup.txt",
                warmupAssocTimeRangeNodes,
                warmupAssocTimeRangeAtypes, warmupAssocTimeRangeTimeLows,
                warmupAssocTimeRangeTimeHighs, warmupAssocTimeRangeLimits);

        readAssocTimeRangeQueries(queryPath + "/assocTimeRange_query.txt",
                assocTimeRangeNodes, assocTimeRangeAtypes,
                assocTimeRangeTimeLows, assocTimeRangeTimeHighs,
                assocTimeRangeLimits);
    }

    @Override
    public int warmupQuery(int i) {
        return g.assocTimeRange(
                modGet(warmupAssocTimeRangeNodes, i),
                modGet(warmupAssocTimeRangeAtypes, i),
                modGet(warmupAssocTimeRangeTimeLows, i),
                modGet(warmupAssocTimeRangeTimeHighs, i),
                modGet(warmupAssocTimeRangeLimits, i)).size();
    }

    @Override
    public int query(int i) {
        return g.assocTimeRange(
                modGet(assocTimeRangeNodes, i),
                modGet(assocTimeRangeAtypes, i),
                modGet(assocTimeRangeTimeLows, i),
                modGet(assocTimeRangeTimeHighs, i),
                modGet(assocTimeRangeLimits, i)).size();
    }
}
