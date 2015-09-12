package edu.berkeley.cs.benchmark;

public class Neighbor extends Benchmark {
    public static final String WARMUP_FILE = "neighbor_warmup_100000.txt";
    public static final String QUERY_FILE = "neighbor_query_100000.txt";

    @Override
    public void readQueries() {
        getLong(WARMUP_FILE, warmupNeighborIds);
        getLong(QUERY_FILE, neighborIds);
    }

    @Override
    public int warmupQuery(int i) {
        return g.getNeighbors(modGet(warmupNeighborIds, i)).size();
    }

    @Override
    public int query(int i) {
        return g.getNeighbors(modGet(neighborIds, i)).size();
    }

}
