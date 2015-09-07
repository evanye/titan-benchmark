package edu.berkeley.cs.benchmark;

import java.util.ArrayList;
import java.util.List;

public class BenchNeighbor {
    private static int WARMUP_N;
    private static int MEASURE_N;

    public static void main(String[] args) {
        String type = args[0];
        String db_path = args[1];
        String warmup_query_path = args[2];
        String query_path = args[3];
        String output_file = args[4];
        WARMUP_N = Integer.parseInt(args[5]);
        MEASURE_N = Integer.parseInt(args[6]);

        List<Long> warmupQueries = new ArrayList<>();
        List<Long> queries = new ArrayList<>();
        BenchUtils.getNeighborQueries(warmup_query_path, warmupQueries);
        BenchUtils.getNeighborQueries(query_path, queries);

        benchNeighborLatency(db_path, warmupQueries, queries, output_file);
    }

    static void benchNeighborLatency(String db_path, List<Long> warmupQueries, List<Long> queries, String output_file) {
    }
}
