package edu.berkeley.cs.benchmark;

import edu.berkeley.cs.titan.Graph;

import java.io.*;
import java.nio.file.Paths;
import java.util.*;

public abstract class Benchmark<T> {
    public static final long SEED = 2331L;

    public static int WARMUP_N;
    public static int MEASURE_N;

    public static long WARMUP_TIME = (long) (60 * 1e9); // 120 seconds
    public static long MEASURE_TIME = (long) (120 * 1e9);
    public static long COOLDOWN_TIME = (long) (30 * 1e9);

    public static final int assocCount_query = 12000000;
    public static final int assocCount_warmup = 2000000;
    public static final int assocGet_query = 12000000;
    public static final int assocGet_warmup = 2000000;
    public static final int assocRange_query = 12000000;
    public static final int assocRange_warmup = 2000000;
    public static final int assocTimeRange_query = 12000000;
    public static final int assocTimeRange_warmup = 2000000;
    public static final int objGet_query = 12000000;
    public static final int objGet_warmup = 2000000;
    public static final int neighborAtype_query = 12000000;
    public static final int neighborAtype_warmup = 2000000;
    public static final int edgeAttr_warmup = neighborAtype_warmup;
    public static final int edgeAttr_query = neighborAtype_warmup;
    public static final int neighborNode_query = 6000000;
    public static final int neighborNode_warmup = 1000000;
    public static final int neighbor_query = 6000000;
    public static final int neighbor_warmup = 1000000;
    public static final int node_query = 6000000;
    public static final int node_warmup = 1000000;

    static String name;
    static String queryPath;
    static String outputPath;
    static String benchClassName;
    static PrintWriter resOut;

    // getNeighbors(n)
    long[] warmupNeighborIds = new long[neighbor_warmup];
    long[] neighborIds = new long[neighbor_query];

    // getNeighborsNode(n, attr)
    long[] warmupNeighborNodeIds = new long[neighborNode_warmup];
    int[] warmupNeighborNodeAttrIds = new int[neighborNode_warmup];
    String[] warmupNeighborNodeAttrs = new String[neighborNode_warmup];
    long[] neighborNodeIds = new long[neighborNode_query];
    int[] neighborNodeAttrIds = new int[neighborNode_query];
    String[] neighborNodeAttrs = new String[neighborNode_query];

    // getNeighbors(n, atype)
    long[] warmupNeighborAtypeIds = new long[neighborAtype_warmup];
    int[] warmupNeighborAtype = new int[neighborAtype_warmup];
    long[] neighborAtypeIds = new long[neighborAtype_query];
    int[] neighborAtype = new int[neighborAtype_query];

    // getEdgeAttr(n, atype)
    long[] warmupEdgeNodeIds = new long[edgeAttr_warmup];
    int[] warmupEdgeAtype = new int[edgeAttr_warmup];
    long[] edgeNodeId = new long[edgeAttr_query];
    int[] edgeAtype = new int[edgeAttr_query];

    // getNodes(attr)
    int[] warmupNodeAttrIds1 = new int[node_warmup];
    String[] warmupNodeAttrs1 = new String[node_warmup];
    int[] nodeAttrIds1 = new int[node_query];
    String[] nodeAttrs1 = new String[node_query];
    // second set for getNodes(attr1, attr2)
    int[] warmupNodeAttrIds2 = new int[node_warmup];
    String[] warmupNodeAttrs2 = new String[node_warmup];
    int[] nodeAttrIds2 = new int[node_query];
    String[] nodeAttrs2 = new String[node_query];

    // assoc_range()
    long[] warmupAssocRangeNodes = new long[assocRange_warmup];
    long[] assocRangeNodes = new long[assocRange_query];
    int[] warmupAssocRangeAtypes = new int[assocRange_warmup];
    int[] assocRangeAtypes = new int[assocRange_query];
    int[] warmupAssocRangeOffsets = new int[assocRange_warmup];
    int[] assocRangeOffsets = new int[assocRange_query];
    int[] warmupAssocRangeLengths = new int[assocRange_warmup];
    int[] assocRangeLengths = new int[assocRange_query];

    // assoc_count()
    long[] warmupAssocCountNodes = new long[assocCount_warmup];
    long[] assocCountNodes = new long[assocCount_query];
    int[] warmupAssocCountAtypes = new int[assocCount_warmup];
    int[] assocCountAtypes = new int[assocCount_query];

    // obj_get()
    long[] warmupObjGetIds = new long[objGet_warmup];
    long[] objGetIds = new long[objGet_query];

    // assoc_get()
    long[] warmupAssocGetNodes = new long[assocGet_warmup];
    long[] assocGetNodes = new long[assocGet_query];
    int[] warmupAssocGetAtypes = new int[assocGet_warmup];
    int[] assocGetAtypes = new int[assocGet_query];
    long[][] warmupAssocGetDstIdSets = new long[assocGet_warmup][];
    long[][] assocGetDstIdSets = new long[assocGet_query][];
    long[] warmupAssocGetTimeLows = new long[assocGet_warmup];
    long[] assocGetTimeLows = new long[assocGet_query];
    long[] warmupAssocGetTimeHighs = new long[assocGet_warmup];
    long[] assocGetTimeHighs = new long[assocGet_query];

    // assoc_time_range()
    long[] warmupAssocTimeRangeNodes = new long[assocTimeRange_warmup];
    long[] assocTimeRangeNodes = new long[assocTimeRange_query];
    int[] warmupAssocTimeRangeAtypes = new int[assocTimeRange_warmup];
    int[] assocTimeRangeAtypes = new int[assocTimeRange_query];
    long[] warmupAssocTimeRangeTimeLows = new long[assocTimeRange_warmup];
    long[] assocTimeRangeTimeLows = new long[assocTimeRange_query];
    long[] warmupAssocTimeRangeTimeHighs = new long[assocTimeRange_warmup];
    long[] assocTimeRangeTimeHighs = new long[assocTimeRange_query];
    int[] warmupAssocTimeRangeLimits = new int[assocTimeRange_warmup];
    int[] assocTimeRangeLimits = new int[assocTimeRange_query];

    public static void main(String[] args) throws Exception {
        benchClassName = args[0];
        String latencyOrThroughput = args[1];
        name = args[2];
        queryPath = args[3];
        outputPath = args[4];

        String fullClassName = Benchmark.class.getPackage().getName() + "." + benchClassName;
        Benchmark b = (Benchmark) Class.forName(fullClassName).newInstance();
        b.readQueries();
        if ("latency".equals(latencyOrThroughput)) {
            if (System.getenv("BENCH_PRINT_RESULTS") != null) {
                resOut = makeFileWriter(benchClassName + ".titan_result", false);
                System.out.println("Logging results to " + benchClassName + ".titan_result");
            }
            WARMUP_N = Integer.parseInt(args[5]);
            MEASURE_N = Integer.parseInt(args[6]);
            b.benchLatency();
        } else if ("throughput".equals(latencyOrThroughput)) {
            int numClients = Integer.parseInt(args[5]);
            WARMUP_TIME = (long) (Integer.parseInt(args[6]) * 1E9);
            MEASURE_TIME = (long) (Integer.parseInt(args[7]) * 1E9);
            COOLDOWN_TIME = (long) (Integer.parseInt(args[8]) * 1E9);
            b.benchThroughput(numClients);
        } else {
            System.err.println("Please choose 'latency' or 'throughput'.");
        }
        System.exit(0);
    }

    public abstract void readQueries();
    public abstract T warmupQuery(Graph g, int i);
    public abstract T query(Graph g, int i);

    /**
     * Returns a throughput job that computes query throughput.
     * Override this in classes where you want to bench the throughput
     * @param clientId
     * @return a class that extends RunThroughput impleemnting the query methods
     */
    public RunThroughput getThroughputJob(int clientId) {
        return null;
    }

    public void benchLatency() {
        Graph g = new Graph();
        PrintWriter out = makeFileWriter(benchClassName + ".csv", false);

        System.out.println("Titan " + benchClassName + " query latency");
        System.out.println("Warming up for " + WARMUP_N + " queries");
        for (int i = 0; i < WARMUP_N; i++) {
            if (i % 10000 == 0) {
                g.restartTransaction();
                System.out.println("Warmed up for " + i + " queries");
            }
            warmupQuery(g, i);
        }

        System.out.println("Measuring for " + MEASURE_N + " queries");
        for (int i = 0; i < MEASURE_N; i++) {
            if (i % 10000 == 0) {
                g.restartTransaction();
                System.out.println("Measured for " + i + " queries");
            }
            long start = System.nanoTime();
            T results = query(g, i);
            long end = System.nanoTime();
            double microsecs = (end - start) / ((double) 1000);

            if (resOut != null) {
                if (results instanceof List<?>) {
                    List<?> resList = (List<?>) results;
                    if (resList.size() > 0 && resList.get(0) instanceof Long) {
                        List<Long> resLongList = (List<Long>) resList;
                        Collections.sort(resLongList);
                        print(resLongList, resOut);
                    } else {
                        for (Object s: resList) {
                            resOut.print("'" + s + "', ");
                        }
                        resOut.println();
                        resOut.flush();
                    }
                } else if (results instanceof Set<?>) {
                    List<Long> resList = new ArrayList<>();
                    resList.addAll((Collection<? extends Long>) results);
                    Collections.sort(resList);
                    print(resList, resOut);
                } else if (results instanceof Long) {
                    resOut.println(results); resOut.flush();
                } else {
                    System.err.println("Invalid result type, has not been handled!");
                }
            }
            long numResults = (results instanceof Collection<?>) ? ((Collection<?>) results).size() : (Long) results;
            out.println(numResults + "," + microsecs);
        }
        out.close();
        Benchmark.printMemoryFootprint();
    }

    public void benchThroughput(int numClients) {
        PrintWriter throughputOut = makeFileWriter(benchClassName + "_throughput.csv", true);
        System.out.println("Titan " + benchClassName + " query throughput with " + numClients + " clients.");

        List<RunThroughput> jobs = new ArrayList<>(numClients);
        List<Thread> clients = new ArrayList<>(numClients);
        for (int i = 0; i < numClients; i++) {
            jobs.add(getThroughputJob(i));
            clients.add(new Thread(jobs.get(i)));
        }
        for (Thread thread : clients) {
            thread.start();
        }
        try {
            for (Thread thread : clients) {
                thread.join();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        double overallQueryThroughput = 0, overallResultThroughput = 0;
        for (RunThroughput j: jobs) {
            overallQueryThroughput += j.getQueryThroughput();
            overallResultThroughput += j.getResultThroughput();
            throughputOut.printf("Client %d\t%f\t%f\n", j.clientId, j.getQueryThroughput(), j.getResultThroughput());
        }
        throughputOut.printf("Overall %s throughput (%d clients)\t%f\t%f\n",
                benchClassName, numClients, overallQueryThroughput, overallResultThroughput);
        throughputOut.close();
        printMemoryFootprint();
    }

    public static <T> void print(Iterable<T> xs, PrintWriter out) {
        if (out == null) return;
        for (T x : xs) {
            out.printf("%s ", x);
        }
        out.println();
        out.flush();
    }

    public static void fullWarmup(Graph g) {
        System.out.println("Warming up graph");
        long start = System.nanoTime();
        g.warmup();
        long end = System.nanoTime();
        printMemoryFootprint();
        System.out.println("Full warmup done in " + (end - start) / 1e6 + " millis");
    }

    public static void printMemoryFootprint() {
        Runtime rt = Runtime.getRuntime();
        long max = rt.maxMemory();
        long allocated = rt.totalMemory();
        System.out.printf(
                "JVM memory: Max %.1f GB, Allocated %.1f GB, Used %.1f GB\n",
                max * 1. / (1L << 30),
                allocated * 1. / (1L << 30),
                (allocated - rt.freeMemory()) * 1. / (1L << 30));
    }

    static void getLong(String file, long[] neighbors) {
        try {
            BufferedReader br = new BufferedReader(new FileReader(queryPath + "/" + file));
            for (int i = 0; i < neighbors.length; i++) {
                String line = br.readLine();
                neighbors[i] = Long.parseLong(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static void getLongInteger(String file, long[] nodeIds, int[] atypes) {
        try {
            BufferedReader br = new BufferedReader(new FileReader(queryPath + "/" + file));
            for (int i = 0; i < nodeIds.length; i++) {
                String line = br.readLine();
                String[] toks = line.split(",");
                nodeIds[i] = Long.valueOf(toks[0]);
                atypes[i] = Integer.valueOf(toks[1]);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static PrintWriter makeFileWriter(String outputName, boolean append) {
        try {
            return new PrintWriter(new BufferedWriter(
                    new FileWriter(Paths.get(outputPath, outputName).toFile(), append)));
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}
