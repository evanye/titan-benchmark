package edu.berkeley.cs.benchmark;

import java.io.PrintWriter;
import java.util.List;
import java.util.Random;
import java.util.Set;

public class Mix extends Benchmark {
    @Override
    public void readQueries() {
        getNeighborQueries(queryPath + "/neighbor_warmup_100000.txt", warmupNeighborIds);
        getNeighborQueries(queryPath + "/neighbor_query_100000.txt", neighborIds);

        getNeighborNodeQueries(queryPath + "/neighbor_node_warmup_100000.txt",
                warmupNeighborNodeIds, warmupNeighborNodeAttrIds, warmupNeighborNodeAttrs);
        getNeighborNodeQueries(queryPath + "/neighbor_node_query_100000.txt",
                neighborNodeIds, neighborNodeAttrIds, neighborNodeAttrs);

        getNeighborAtypeQueries(queryPath + "/neighborAtype_warmup_100000.txt",
                warmupNeighborAtypeIds, warmupNeighborAtype);
        getNeighborAtypeQueries(queryPath + "/neighborAtype_query_100000.txt",
                neighborAtypeIds, neighborAtype);

        getNodeQueries(queryPath + "/node_warmup_100000.txt",
                warmupNodeAttrIds1, warmupNodeAttrIds2, warmupNodeAttrs1, warmupNodeAttrs2);
        getNodeQueries(queryPath + "/node_query_100000.txt", nodeAttrIds1, nodeAttrIds2, nodeAttrs1, nodeAttrs2);
    }

    @Override
    public void benchLatency() {
        PrintWriter neighborOut = makeFileWriter(g.getName() + "_mix_neighbor.csv");
        PrintWriter neighborNodeOut = makeFileWriter(g.getName() + "_mix_neighbor_node.csv");
        PrintWriter neighborAtypeOut = makeFileWriter(g.getName() + "_mix_neighbor_atype.csv");
        PrintWriter nodeOut = makeFileWriter(g.getName() + "_mix_node.csv");
        PrintWriter nodeNodeOut = makeFileWriter(g.getName() + "_mix_node_node.csv");

        long seed = 1618L; int randQuery;
        Random rand = new Random(seed);

        System.out.println("Titan mix query latency");
//        Benchmark.fullWarmup(g);
        System.out.println("Warming up for " + WARMUP_N + " queries");
        for (int i = 0; i < WARMUP_N; i++) {
            if (i % 10000 == 0) {
                g.restartTransaction();
                System.out.println("Warmed up for " + i + " queries");
            }

            randQuery = rand.nextInt(5);
            switch(randQuery) {
                case 0:
                    g.getNeighbors(modGet(warmupNeighborIds, i));
                    break;
                case 1:
                    g.getNeighborNode(modGet(warmupNeighborNodeIds, i),
                            modGet(warmupNeighborNodeAttrIds, i), modGet(warmupNeighborNodeAttrs, i));
                    break;
                case 2:
                    g.getNeighborAtype(modGet(warmupNeighborAtypeIds, i), modGet(warmupNeighborAtype, i));
                    break;
                case 3:
                    g.getNodes(modGet(warmupNodeAttrIds1, i), modGet(warmupNodeAttrs1, i));
                    break;
                case 4:
                    g.getNodes(modGet(warmupNodeAttrIds1, i), modGet(warmupNodeAttrs1, i),
                            modGet(warmupNodeAttrIds2, i), modGet(warmupNodeAttrs2, i));
                    break;
            }
        }

        rand.setSeed(1618L); // re-seed
        long start, end;

        System.out.println("Measuring for " + MEASURE_N + " queries");
        for (int i = 0; i < MEASURE_N; i++) {
            if (i % 10000 == 0) {
                g.restartTransaction();
                System.out.println("Measured for " + i + " queries");
            }
            randQuery = rand.nextInt(5);
            switch(randQuery) {
                case 0:
                    start = System.nanoTime();
                    List<Long> neighbors = g.getNeighbors(modGet(neighborIds, i));
                    end = System.nanoTime();
                    neighborOut.println(neighbors.size() + "," + (end - start) / (1000.0));
                    break;
                case 1:
                    start = System.nanoTime();
                    List<Long> neighborNodes = g.getNeighborNode(modGet(neighborNodeIds, i),
                            modGet(neighborNodeAttrIds, i), modGet(neighborNodeAttrs, i));
                    end = System.nanoTime();
                    neighborNodeOut.println(neighborNodes.size() + "," + (end - start) / (1000.0));
                    break;
                case 2:
                    start = System.nanoTime();
                    List<Long> neighborAtypes = g.getNeighborAtype(modGet(neighborAtypeIds, i), modGet(neighborAtype, i));
                    end = System.nanoTime();
                    neighborAtypeOut.println(neighborAtypes.size() + "," + (end - start) / 1000.0);
                    break;
                case 3:
                    start = System.nanoTime();
                    Set<Long> nodes = g.getNodes(modGet(nodeAttrIds1, i), modGet(nodeAttrs1, i));
                    end = System.nanoTime();
                    nodeOut.println(nodes.size() + "," + (end - start) / 1000.0);
                    break;
                case 4:
                    start = System.nanoTime();
                    Set<Long> nodeNodes = g.getNodes(modGet(nodeAttrIds1, i), modGet(nodeAttrs1, i),
                            modGet(nodeAttrIds2, i), modGet(nodeAttrs2, i));
                    end = System.nanoTime();
                    nodeNodeOut.println(nodeNodes.size() + "," + (end - start) / 1000.0);
                    break;
            }
        }

        neighborOut.close();
        neighborNodeOut.close();
        neighborAtypeOut.close();
        nodeOut.close();
        nodeNodeOut.close();
        printMemoryFootprint();
    }

    @Override
    public void benchThroughput() {

    }
}
