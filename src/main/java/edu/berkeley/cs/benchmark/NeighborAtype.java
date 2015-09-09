package edu.berkeley.cs.benchmark;

import java.io.PrintWriter;
import java.util.List;

public class NeighborAtype extends Benchmark {
    @Override
    public void readQueries() {
        getNeighborAtypeQueries(queryPath + "/neighborAtype_warmup_100000.txt",
                warmupNeighborAtypeIds, warmupNeighborAtype);
        getNeighborAtypeQueries(queryPath + "/neighborAtype_query_100000.txt",
                neighborAtypeIds, neighborAtype);
    }

    @Override
    public void benchLatency() {
        PrintWriter out = makeFileWriter(g.getName() + "_" + "neighbor_node.csv");
        System.out.println("Titan getNeighborAtype query latency");
//        Benchmark.fullWarmup(g);
        System.out.println("Warming up for " + WARMUP_N + " queries");
        for (int i = 0; i < WARMUP_N; i++) {
            if (i % 10000 == 0) {
                g.restartTransaction();
                System.out.println("Warmed up for " + i + " queries");
            }
            List<Long> nodes = g.getNeighborAtype(modGet(warmupNeighborAtypeIds, i), modGet(warmupNeighborAtype, i));
        }

        System.out.println("Measuring for " + MEASURE_N + " queries");
        for (int i = 0; i < MEASURE_N; i++) {
            if (i % 10000 == 0) {
                g.restartTransaction();
                System.out.println("Measured for " + i + " queries");
            }
            long start = System.nanoTime();
            List<Long> nodes = g.getNeighborAtype(modGet(neighborAtypeIds, i), modGet(neighborAtype, i));
            long end = System.nanoTime();
            double microsecs = (end - start) / ((double) 1000);
            out.println(nodes.size() + "," + microsecs);
        }
        out.close();
        printMemoryFootprint();
    }

    @Override
    public void benchThroughput() {

    }
}
