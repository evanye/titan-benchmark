package edu.berkeley.cs.benchmark;

import edu.berkeley.cs.Graph;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import static edu.berkeley.cs.benchmark.BenchUtils.*;

public class BenchNeighbor {
    private static int WARMUP_N;
    private static int MEASURE_N;

    public static void main(String[] args) {
        String type = args[0];
        String db_path = args[1];
        String output_file = args[2];
        WARMUP_N = Integer.parseInt(args[3]);
        MEASURE_N = Integer.parseInt(args[4]);
        String query_path = args[5];

        Graph g = new Graph();
        List<Long> warmup_queries = new ArrayList<>();
        List<Long> queries = new ArrayList<>();
        getNeighborQueries(query_path, warmup_queries, queries);
        PrintWriter out = makeFileWriter(output_file);

        benchNeighborLatency(g, out, warmup_queries, queries);
    }

    static void benchNeighborLatency(Graph g, PrintWriter out, List<Long> warmupQueries, List<Long> queries) {
        System.out.println("Titan getNeighbor query latency");
        BenchUtils.fullWarmup(g);
        System.out.println("Warming up for " + WARMUP_N + " queries");
        for (int i = 0; i < WARMUP_N; i++) {
            if (i % 10000 == 0) {
                g.restartTransaction();
                System.out.println("Warmed up for " + i + " queries");
            }
            List<Long> neighbors = g.getNeighbors(modGet(warmupQueries, i));
        }

        System.out.println("Measuring for " + MEASURE_N + " queries");
        for (int i = 0; i < MEASURE_N; i++) {
            if (i % 10000 == 0) {
                g.restartTransaction();
                System.out.println("Measured for " + i + " queries");
            }
            long start = System.nanoTime();
            List<Long> neighbors = g.getNeighbors(modGet(warmupQueries, i));
            long end = System.nanoTime();
            double microsecs = (end - start) / ((double) 1000);
            out.println(neighbors.size() + "," + microsecs);
        }
        out.close();
        printMemoryFootprint();
    }
}
