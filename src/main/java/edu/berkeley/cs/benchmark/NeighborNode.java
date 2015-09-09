package edu.berkeley.cs.benchmark;

import java.io.PrintWriter;
import java.util.List;

public class NeighborNode extends Benchmark {
    @Override
    public void readQueries() {
        getNeighborNodeQueries(queryPath + "/neighbor_node_warmup_100000.txt",
                warmupNeighborNodeIds, warmupNeighborNodeAttrIds, warmupNeighborNodeAttrs);
        getNeighborNodeQueries(queryPath + "/neighbor_node_query_100000.txt",
                neighborNodeIds, neighborNodeAttrIds, neighborNodeAttrs);
    }

    @Override
    public void benchLatency() {
        PrintWriter out = makeFileWriter(g.getName() + "_" + "neighbor_node.csv");
        System.out.println("Titan getNeighborNode query latency");
//        Benchmark.fullWarmup(g);
        System.out.println("Warming up for " + WARMUP_N + " queries");
        for (int i = 0; i < WARMUP_N; i++) {
            if (i % 10000 == 0) {
                g.restartTransaction();
                System.out.println("Warmed up for " + i + " queries");
            }
            List<Long> nodes = g.getNeighborNode(modGet(warmupNeighborNodeIds, i),
                    modGet(warmupNeighborNodeAttrIds, i), modGet(warmupNeighborNodeAttrs, i));
        }

        System.out.println("Measuring for " + MEASURE_N + " queries");
        for (int i = 0; i < MEASURE_N; i++) {
            if (i % 10000 == 0) {
                g.restartTransaction();
                System.out.println("Measured for " + i + " queries");
            }
            long start = System.nanoTime();
            List<Long> nodes = g.getNeighborNode(modGet(neighborNodeIds, i),
                    modGet(neighborNodeAttrIds, i), modGet(neighborNodeAttrs, i));
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
