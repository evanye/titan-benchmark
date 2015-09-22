package edu.berkeley.cs.benchmark;

import edu.berkeley.cs.titan.Graph;

import java.io.PrintWriter;
import java.util.*;

public class MixPrimitive extends Benchmark<Object> {

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

        EdgeAttr.getLongInteger(EdgeAttr.WARMUP_FILE, warmupEdgeNodeIds, warmupEdgeAtype);
        EdgeAttr.getLongInteger(EdgeAttr.QUERY_FILE, edgeNodeId, edgeAtype);
    }

    @Override
    public void benchLatency() {
        Graph graph = new Graph();
        PrintWriter neighborOut = makeFileWriter("mix_Neighbor.csv", false);
        PrintWriter neighborNodeOut = makeFileWriter("mix_NeighborNode.csv", false);
        PrintWriter neighborAtypeOut = makeFileWriter("mix_NeighborAtype.csv", false);
        PrintWriter nodeOut = makeFileWriter("mix_Node.csv", false);
        PrintWriter nodeNodeOut = makeFileWriter("mix_NodeNode.csv", false);

        int randQuery;
        Random rand = new Random(SEED);

        System.out.println("Titan mix query latency");
        System.out.println("Warming up for " + WARMUP_N + " queries");
        for (int i = 0; i < WARMUP_N; i++) {
            if (i % 10000 == 0) {
                graph.restartTransaction();
                System.out.println("Warmed up for " + i + " queries");
            }

            randQuery = rand.nextInt(5);
            switch(randQuery) {
                case 0:
                    graph.getNeighbors(warmupNeighborIds[i]);
                    break;
                case 1:
                    graph.getNeighborNode(warmupNeighborNodeIds[i],
                            warmupNeighborNodeAttrIds[i], warmupNeighborNodeAttrs[i]);
                    break;
                case 2:
                    graph.getNeighborAtype(warmupNeighborAtypeIds[i], warmupNeighborAtype[i]);
                    break;
                case 3:
//                    graph.getNodes(warmupNodeAttrIds1[i], warmupNodeAttrs1[i]);
                    graph.getEdgeAttrs(warmupEdgeNodeIds[i], warmupEdgeAtype[i]);
                    break;
                case 4:
                    graph.getNodes(warmupNodeAttrIds1[i], warmupNodeAttrs1[i],
                            warmupNodeAttrIds2[i], warmupNodeAttrs2[i]);
                    break;
            }
        }

        rand.setSeed(SEED); // re-seed
        long start, end;

        System.out.println("Measuring for " + MEASURE_N + " queries");
        for (int i = 0; i < MEASURE_N; i++) {
            if (i % 10000 == 0) {
                graph.restartTransaction();
                System.out.println("Measured for " + i + " queries");
            }
            randQuery = rand.nextInt(5);
            switch(randQuery) {
                case 0:
                    start = System.nanoTime();
                    List<Long> neighbors = graph.getNeighbors(neighborIds[i]);
                    end = System.nanoTime();
                    Collections.sort(neighbors);
                    print(neighbors, resOut);
                    neighborOut.println(neighbors.size() + "," + (end - start) / (1000.0));
                    break;
                case 1:
                    start = System.nanoTime();
                    List<Long> neighborNodes = graph.getNeighborNode(neighborNodeIds[i],
                            neighborNodeAttrIds[i], neighborNodeAttrs[i]);
                    end = System.nanoTime();
                    Collections.sort(neighborNodes);
                    print(neighborNodes, resOut);
                    neighborNodeOut.println(neighborNodes.size() + "," + (end - start) / (1000.0));
                    break;
                case 2:
                    start = System.nanoTime();
                    List<Long> neighborAtypes = graph.getNeighborAtype(neighborAtypeIds[i], neighborAtype[i]);
                    end = System.nanoTime();
                    Collections.sort(neighborAtypes);
                    print(neighborAtypes, resOut);
                    neighborAtypeOut.println(neighborAtypes.size() + "," + (end - start) / 1000.0);
                    break;
                case 3:
                    start = System.nanoTime();
//                    Set<Long> nodes = graph.getNodes(nodeAttrIds1[i], nodeAttrs1[i]);
                    List<String> edgeAttrs = graph.getEdgeAttrs(edgeNodeId[i], edgeAtype[i]);
                    end = System.nanoTime();
                    Collections.sort(edgeAttrs);
                    print(edgeAttrs, resOut);
                    nodeOut.println(edgeAttrs.size() + "," + (end - start) / 1000.0);
                    break;
                case 4:
                    start = System.nanoTime();
                    Set<Long> nodeNodes = graph.getNodes(nodeAttrIds1[i], nodeAttrs1[i],
                            nodeAttrIds2[i], nodeAttrs2[i]);
                    end = System.nanoTime();
                    List<Long> results = new ArrayList<>(nodeNodes);
                    print(results, resOut);
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
    public RunThroughput getThroughputJob(int clientId) {
        return new RunThroughput(clientId) {
            @Override
            public void warmupQuery() {
                int randQuery = rand.nextInt(5), queryIdx;
                switch (randQuery) {
                    case 0:
                        queryIdx = rand.nextInt(neighbor_warmup);
                        g.getNeighbors(warmupNeighborIds[queryIdx]);
                        break;
                    case 1:
                        queryIdx = rand.nextInt(neighborNode_warmup);
                        g.getNeighborNode(
                                warmupNeighborNodeIds[queryIdx],
                                warmupNeighborNodeAttrIds[queryIdx],
                                warmupNeighborNodeAttrs[queryIdx]);
                        break;
                    case 2:
//                        queryIdx = rand.nextInt(warmupNodeAttrIds1.size());
//                        g.getNodes(warmupNodeAttrIds1[queryIdx],
//                                warmupNodeAttrs1[queryIdx]);
                        queryIdx = rand.nextInt(edgeAttr_warmup);
                        g.getEdgeAttrs(warmupEdgeNodeIds[queryIdx], warmupEdgeAtype[queryIdx]);
                        break;
                    case 3:
                        queryIdx = rand.nextInt(neighborAtype_warmup);
                        g.getNeighborAtype(warmupNeighborAtypeIds[queryIdx],
                                warmupNeighborAtype[queryIdx]);
                        break;
                    case 4:
                        queryIdx = rand.nextInt(node_warmup);
                        g.getNodes(warmupNodeAttrIds1[queryIdx],
                                warmupNodeAttrs1[queryIdx],
                                warmupNodeAttrIds2[queryIdx],
                                warmupNodeAttrs2[queryIdx]);
                        break;
                }
            }

            @Override
            public int query() {
                int queryIdx;
                int randQuery = rand.nextInt(5);
                switch (randQuery) {
                    case 0:
                        // get_nhbrs(n)
                        queryIdx = rand.nextInt(neighbor_query);
                        return g.getNeighbors(neighborIds[queryIdx]).size();
                    case 1:
                        // get_nhbrs(n, attr)
                        queryIdx = rand.nextInt(neighborNode_query);
                        return g.getNeighborNode(
                                neighborNodeIds[queryIdx],
                                neighborNodeAttrIds[queryIdx],
                                neighborNodeAttrs[queryIdx]).size();
                    case 2:
                        // get_nodes(attr)
//                        queryIdx = rand.nextInt(nodeAttrIds1.size());
//                        return g.getNodes(
//                                nodeAttrIds1[queryIdx],
//                                nodeAttrs1[queryIdx]).size();
                        queryIdx = rand.nextInt(edgeAttr_query);
                        return g.getEdgeAttrs(edgeNodeId[queryIdx],
                                edgeAtype[queryIdx]).size();
                    case 3:
                        // get_nhbrs(n, atype)
                        queryIdx = rand.nextInt(neighborAtype_query);
                        return g.getNeighborAtype(
                                neighborAtypeIds[queryIdx],
                                neighborAtype[queryIdx]).size();
                    case 4:
                        // get_nodes(attr1, attr2)
                        queryIdx = rand.nextInt(node_query);
                        return g.getNodes(
                                nodeAttrIds1[queryIdx],
                                nodeAttrs1[queryIdx],
                                nodeAttrIds2[queryIdx],
                                nodeAttrs2[queryIdx]).size();
                }
                return Integer.MIN_VALUE;
            }
        };
    }

    /**
     * These queries are not being used, since benchLatency is overriden.
     */

    @Override
    public Object warmupQuery(Graph g, int i) {
        return null;
    }

    @Override
    public Object query(Graph g, int i) {
        return null;
    }

}

