package edu.berkeley.cs.benchmark;

import java.io.PrintWriter;
import java.util.Collection;
import java.util.List;

public class Neighbor extends Benchmark {
    @Override
    public void readQueries() {
        getNeighborQueries(queryPath + "/neighbor_warmup_100000.txt", warmupNeighborIds);
        getNeighborQueries(queryPath + "/neighbor_query_100000.txt", neighborIds);
    }

    @Override
    public void benchLatency() {
        PrintWriter out = makeFileWriter(g.getName() + "_neighbor.csv");
        System.out.println("Titan getNeighbor query latency");
        //fullWarmup(g);
        System.out.println("Warming up for " + WARMUP_N + " queries");
        for (int i = 0; i < WARMUP_N; i++) {
            if (i % 10000 == 0) {
                g.restartTransaction();
                System.out.println("Warmed up for " + i + " queries");
            }
            List<Long> neighbors = g.getNeighbors(modGet(warmupNeighborIds, i));
        }

        System.out.println("Measuring for " + MEASURE_N + " queries");
        for (int i = 0; i < MEASURE_N; i++) {
            if (i % 10000 == 0) {
                g.restartTransaction();
                System.out.println("Measured for " + i + " queries");
            }
            long start = System.nanoTime();
            List<Long> neighbors = g.getNeighbors(modGet(neighborIds, i));
            long end = System.nanoTime();
            double microsecs = (end - start) / ((double) 1000);
            out.println(neighbors.size() + "," + microsecs);
        }
        out.close();
        printMemoryFootprint();
    }

    @Override
    public Collection<?> warmupQuery(int i) {
        return g.getNeighbors(modGet(warmupNeighborIds, i));
    }

    @Override
    public Collection<?> query(int i) {
        return g.getNeighbors(modGet(neighborIds, i));
    }
}
