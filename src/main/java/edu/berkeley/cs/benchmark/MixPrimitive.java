package edu.berkeley.cs.benchmark;

import java.io.PrintWriter;
import java.util.List;
import java.util.Random;
import java.util.Set;

public class MixPrimitive extends Benchmark {
    private static final long SEED = 1618L;

    @Override
    public void readQueries() {
        getLong(Neighbor.WARMUP_FILE, warmupNeighborIds);
        getLong(Neighbor.QUERY_FILE, neighborIds);

        Node.getNodeQueries(Node.WARMUP_FILE,
                warmupNodeAttrIds1, warmupNodeAttrIds2, warmupNodeAttrs1, warmupNodeAttrs2);
        Node.getNodeQueries(Node.QUERY_FILE,
                nodeAttrIds1, nodeAttrIds2, nodeAttrs1, nodeAttrs2);

        NeighborNode.getNeighborNodeQueries(NeighborNode.WARMUP_FILE,
                warmupNeighborNodeIds, warmupNeighborNodeAttrIds, warmupNeighborNodeAttrs);
        NeighborNode.getNeighborNodeQueries(NeighborNode.QUERY_FILE,
                neighborNodeIds, neighborNodeAttrIds, neighborNodeAttrs);

        NeighborAtype.getLongInteger(NeighborAtype.WARMUP_FILE, warmupNeighborAtypeIds, warmupNeighborAtype);
        NeighborAtype.getLongInteger(NeighborAtype.QUERY_FILE, neighborAtypeIds, neighborAtype);
    }

    @Override
    public void benchLatency() {
        PrintWriter neighborOut = makeFileWriter(g.getName() + "_mix_Neighbor.csv");
        PrintWriter neighborNodeOut = makeFileWriter(g.getName() + "_mix_NeighborNode.csv");
        PrintWriter neighborAtypeOut = makeFileWriter(g.getName() + "_mix_NeighborAtype.csv");
        PrintWriter nodeOut = makeFileWriter(g.getName() + "_mix_Node.csv");
        PrintWriter nodeNodeOut = makeFileWriter(g.getName() + "_mix_NodeNode.csv");

        int randQuery;
        Random rand = new Random(SEED);

        System.out.println("Titan mix query latency");
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

        rand.setSeed(SEED); // re-seed
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
        int randQuery;
        Random rand = new Random(SEED);

        System.out.println("Titan mix query throughput");
        System.out.println("Warming up for " + WARMUP_TIME + " nanoseconds");
        int i = 0;
        long warmupStart = System.nanoTime();
        while (System.nanoTime() - warmupStart < WARMUP_TIME) {
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
            ++i;
        }

        System.out.println("Measuring for " + MEASURE_TIME + " nanoseconds");
        i = 0;
        rand.setSeed(SEED); // re-seed
        long start = System.nanoTime();
        while (System.nanoTime() - start < MEASURE_TIME) {
            if (i % 10000 == 0) {
                g.restartTransaction();
            }
            randQuery = rand.nextInt(5);
            switch(randQuery) {
                case 0:
                    List<Long> neighbors = g.getNeighbors(modGet(neighborIds, i));
                    break;
                case 1:
                    List<Long> neighborNodes = g.getNeighborNode(modGet(neighborNodeIds, i),
                            modGet(neighborNodeAttrIds, i), modGet(neighborNodeAttrs, i));
                    break;
                case 2:
                    List<Long> neighborAtypes = g.getNeighborAtype(modGet(neighborAtypeIds, i), modGet(neighborAtype, i));
                    break;
                case 3:
                    Set<Long> nodes = g.getNodes(modGet(nodeAttrIds1, i), modGet(nodeAttrs1, i));
                    break;
                case 4:
                    Set<Long> nodeNodes = g.getNodes(modGet(nodeAttrIds1, i), modGet(nodeAttrs1, i),
                            modGet(nodeAttrIds2, i), modGet(nodeAttrs2, i));
                    break;
            }
            ++i;
        }
        double totalSeconds = (System.nanoTime() - start) * 1. / 1e9;
        double queryThroughput = ((double) i) / totalSeconds;
        throughputOut.println("MixPrimitive Throughput: " + queryThroughput);
        throughputOut.close();

        printMemoryFootprint();
    }

    @Override
    public int warmupQuery(int i) {
        return Integer.MIN_VALUE;
    }

    @Override
    public int query(int i) {
        return Integer.MIN_VALUE;
    }

}
