package edu.berkeley.cs.benchmark;

import java.io.PrintWriter;
import java.util.Collection;
import java.util.Set;

public class NodeNode extends Benchmark {

    @Override
    public void readQueries() {
        getNodeQueries(queryPath + "/node_warmup_100000.txt",
                warmupNodeAttrIds1, warmupNodeAttrIds2, warmupNodeAttrs1, warmupNodeAttrs2);
        getNodeQueries(queryPath + "/node_query_100000.txt", nodeAttrIds1, nodeAttrIds2, nodeAttrs1, nodeAttrs2);
    }

    @Override
    public void benchLatency() {
        PrintWriter out = makeFileWriter(g.getName() + "_" + "node_node.csv");
        System.out.println("Titan getNodeNode query latency");
//        Benchmark.fullWarmup(g);
        System.out.println("Warming up for " + WARMUP_N + " queries");
        for (int i = 0; i < WARMUP_N; i++) {
            if (i % 10000 == 0) {
                g.restartTransaction();
                System.out.println("Warmed up for " + i + " queries");
            }
            Set<Long> nodes = g.getNodes(modGet(warmupNodeAttrIds1, i), modGet(warmupNodeAttrs1, i),
                    modGet(warmupNodeAttrIds2, i), modGet(warmupNodeAttrs2, i));
        }

        System.out.println("Measuring for " + MEASURE_N + " queries");
        for (int i = 0; i < MEASURE_N; i++) {
            if (i % 10000 == 0) {
                g.restartTransaction();
                System.out.println("Measured for " + i + " queries");
            }
            long start = System.nanoTime();
            Set<Long> nodes = g.getNodes(modGet(nodeAttrIds1, i), modGet(nodeAttrs1, i),
                    modGet(nodeAttrIds2, i), modGet(nodeAttrs2, i));
            long end = System.nanoTime();
            double microsecs = (end - start) / ((double) 1000);
            out.println(nodes.size() + "," + microsecs);
        }
        out.close();
        printMemoryFootprint();
    }

    @Override
    public Collection<?> warmupQuery(int i) {
        return g.getNodes(modGet(warmupNodeAttrIds1, i), modGet(warmupNodeAttrs1, i),
                modGet(warmupNodeAttrIds2, i), modGet(warmupNodeAttrs2, i));
    }

    @Override
    public Collection<?> query(int i) {
        return g.getNodes(modGet(nodeAttrIds1, i), modGet(nodeAttrs1, i),
                modGet(nodeAttrIds2, i), modGet(nodeAttrs2, i));
    }

}
