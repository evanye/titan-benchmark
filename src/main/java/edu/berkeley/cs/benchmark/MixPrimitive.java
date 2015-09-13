package edu.berkeley.cs.benchmark;

import edu.berkeley.cs.titan.Graph;

import java.io.PrintWriter;
import java.util.ArrayList;
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
        PrintWriter neighborOut = makeFileWriter(name + "_mix_Neighbor.csv");
        PrintWriter neighborNodeOut = makeFileWriter(name + "_mix_NeighborNode.csv");
        PrintWriter neighborAtypeOut = makeFileWriter(name + "_mix_NeighborAtype.csv");
        PrintWriter nodeOut = makeFileWriter(name + "_mix_Node.csv");
        PrintWriter nodeNodeOut = makeFileWriter(name + "_mix_NodeNode.csv");

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
    public void benchThroughput(int numClients) {
        System.out.println("Titan mix query throughput with " + numClients + " clients.");
        try {
            List<Thread> clients = new ArrayList<>(numClients);
            for (int i = 0; i < numClients; i++) {
                clients.add(new Thread(new RunMixThroughput(i)));
            }
            for (Thread thread : clients) {
                thread.start();
            }
            for (Thread thread : clients) {
                thread.join();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        throughputOut.close();
        printMemoryFootprint();
    }

    class RunMixThroughput implements Runnable {
        private int clientId;
        private Random rand;
        private Graph g;

        public RunMixThroughput(int clientId) {
            this.clientId = clientId;
            rand = new Random(SEED + clientId);
            g = new Graph();
        }

        private void runWarmup() {
            int randQuery = rand.nextInt(5), queryIdx;
            switch (randQuery) {
                case 0:
                    queryIdx = rand.nextInt(warmupNeighborIds.size());
                    g.getNeighbors(modGet(warmupNeighborIds, queryIdx));
                    break;
                case 1:
                    queryIdx = rand.nextInt(warmupNeighborNodeIds.size());
                    g.getNeighborNode(
                            modGet(warmupNeighborNodeIds, queryIdx),
                            modGet(warmupNeighborNodeAttrIds, queryIdx),
                            modGet(warmupNeighborNodeAttrs, queryIdx));
                    break;
                case 2:
                    queryIdx = rand.nextInt(warmupNodeAttrIds1.size());
                    g.getNodes(modGet(warmupNodeAttrIds1, queryIdx),
                            modGet(warmupNodeAttrs1, queryIdx));
                    break;
                case 3:
                    queryIdx = rand.nextInt(warmupNeighborAtypeIds.size());
                    g.getNeighborAtype(modGet(warmupNeighborAtypeIds, queryIdx),
                                       modGet(warmupNeighborAtype, queryIdx));
                    break;
                case 4:
                    queryIdx = rand.nextInt(warmupNodeAttrIds1.size());
                    g.getNodes(modGet(warmupNodeAttrIds1, queryIdx),
                            modGet(warmupNodeAttrs1, queryIdx),
                            modGet(warmupNodeAttrIds2, queryIdx),
                            modGet(warmupNodeAttrs2, queryIdx));
                    break;
            }
        }

        @Override
        public void run() {
            int randQuery;
            // warmup
            int i = 0, queryIdx;
            long warmupStart = System.nanoTime();
            System.out.println("Client " + clientId + " warming up for " + (WARMUP_TIME / 1E9) + " seconds.");
            while (System.nanoTime() - warmupStart < WARMUP_TIME) {
                if (i % 10000 == 0) {
                    g.restartTransaction();
                }
                runWarmup();
            }
            System.out.println("Client " + clientId + " finished warming up!");

            // measure
            i = 0;
            long results = 0;
            System.out.println("Client " + clientId + " measuring for " + (MEASURE_TIME / 1E9) + " seconds.");
            long start = System.nanoTime();
            while (System.nanoTime() - start < MEASURE_TIME) {
                if (i % 10000 == 0) {
                    g.restartTransaction();
                }

                randQuery = rand.nextInt(5);
                switch (randQuery) {
                    case 0:
                        // get_nhbrs(n)
                        queryIdx = rand.nextInt(neighborIds.size());
                        results += g.getNeighbors(modGet(neighborIds, queryIdx)).size();
                        break;
                    case 1:
                        // get_nhbrs(n, attr)
                        queryIdx = rand.nextInt(neighborNodeIds.size());
                        results += g.getNeighborNode(
                                modGet(neighborNodeIds, queryIdx),
                                modGet(neighborNodeAttrIds, queryIdx),
                                modGet(neighborNodeAttrs, queryIdx)).size();
                        break;
                    case 2:
                        // get_nodes(attr)
                        queryIdx = rand.nextInt(nodeAttrIds1.size());
                        results += g.getNodes(
                                modGet(nodeAttrIds1, queryIdx),
                                modGet(nodeAttrs1, queryIdx)).size();
                        break;
                    case 3:
                        // get_nhbrs(n, atype)
                        queryIdx = rand.nextInt(neighborAtype.size());
                        results += g.getNeighborAtype(
                                modGet(neighborAtypeIds, queryIdx),
                                modGet(neighborAtype, queryIdx)).size();
                        break;
                    case 4:
                        // get_nodes(attr1, attr2)
                        queryIdx = rand.nextInt(nodeAttrIds1.size());
                        results += g.getNodes(
                                modGet(nodeAttrIds1, queryIdx),
                                modGet(nodeAttrs1, queryIdx),
                                modGet(nodeAttrIds2, queryIdx),
                                modGet(nodeAttrs2, queryIdx)).size();
                        break;
                }
                ++i;
            }
            long end = System.nanoTime();
            System.out.println("Client " + clientId + " finished measuring!");
            double totalSeconds = (end - start) * 1. / 1e9;
            double queryThput = ((double) i) / totalSeconds;
            double resultsThput = ((double) results ) / totalSeconds;

            throughputOut.printf("%s,%f,%f\n", "Client " + clientId, queryThput, resultsThput);

            System.out.println("Client " + clientId + " cooling down for " + (COOLDOWN_TIME / 1E9) + " seconds.");
            long cooldownStart = System.nanoTime();
            while (System.nanoTime() - cooldownStart < COOLDOWN_TIME) {
                runWarmup();
            }
            System.out.println("Client " + clientId + " finished cooling down!");
        }
    }

    @Override
    public int warmupQuery(int i) {
        return -1;
    }

    @Override
    public int query(int i) {
        return -1;
    }

}

