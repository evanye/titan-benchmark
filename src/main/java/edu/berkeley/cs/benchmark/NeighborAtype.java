package edu.berkeley.cs.benchmark;

public class NeighborAtype extends Benchmark {
    public static final String WARMUP_FILE = "neighborAtype_warmup_100000.txt";
    public static final String QUERY_FILE = "neighborAtype_query_100000.txt";

    @Override
    public void readQueries() {
        getLongInteger(WARMUP_FILE, warmupNeighborAtypeIds, warmupNeighborAtype);
        getLongInteger(QUERY_FILE, neighborAtypeIds, neighborAtype);
    }

    @Override
    public int warmupQuery(int i) {
        return g.getNeighborAtype(modGet(warmupNeighborAtypeIds, i), modGet(warmupNeighborAtype, i)).size();
    }

    @Override
    public int query(int i) {
        return g.getNeighborAtype(modGet(neighborAtypeIds, i), modGet(neighborAtype, i)).size();
    }

}
