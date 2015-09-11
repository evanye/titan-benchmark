package edu.berkeley.cs.benchmark.tao;

public class AssocRange extends BenchTao {
    @Override
    public void readQueries() {
        readAssocRangeQueries(queryPath + "/assocRange_warmup.txt",
                warmupAssocRangeNodes, warmupAssocRangeAtypes,
                warmupAssocRangeOffsets, warmupAssocRangeLengths);
        readAssocRangeQueries(queryPath + "/assocRange_query.txt",
                assocRangeNodes, assocRangeAtypes,
                assocRangeOffsets, assocRangeLengths);
    }

    @Override
    public int warmupQuery(int i) {
        return g.assocRange(
                modGet(warmupAssocRangeNodes, i),
                modGet(warmupAssocRangeAtypes, i),
                modGet(warmupAssocRangeOffsets, i),
                modGet(warmupAssocRangeLengths, i)).size();
    }

    @Override
    public int query(int i) {
        return g.assocRange(
                modGet(assocRangeNodes, i),
                modGet(assocRangeAtypes, i),
                modGet(assocRangeOffsets, i),
                modGet(assocRangeLengths, i)).size();
    }
}
