package edu.berkeley.cs.benchmark;

import edu.berkeley.cs.titan.Graph;

import java.io.*;
import java.nio.file.Paths;
import java.util.*;

public abstract class Benchmark<T> {
    public static final long SEED = 2331L;

    public static int WARMUP_N;
    public static int MEASURE_N;

    static String name;
    static String queryPath;
    static String outputPath;
    static String benchClassName;
    static PrintWriter resOut;

    // getNeighbors(n)
    List<Long> warmupNeighborIds = new ArrayList<>();
    List<Long> neighborIds = new ArrayList<>();

    // getNeighborsNode(n, attr)
    List<Long> warmupNeighborNodeIds = new ArrayList<>();
    List<Integer> warmupNeighborNodeAttrIds = new ArrayList<>();
    List<String> warmupNeighborNodeAttrs = new ArrayList<>();
    List<Long> neighborNodeIds = new ArrayList<>();
    List<Integer> neighborNodeAttrIds = new ArrayList<>();
    List<String> neighborNodeAttrs = new ArrayList<>();

    // getNeighbors(n, atype)
    List<Long> warmupNeighborAtypeIds = new ArrayList<>();
    List<Integer> warmupNeighborAtype = new ArrayList<>();
    List<Long> neighborAtypeIds = new ArrayList<>();
    List<Integer> neighborAtype = new ArrayList<>();

    // getEdgeAttr(n, atype)
    List<Long> warmupEdgeNodeIds = new ArrayList<>();
    List<Integer> warmupEdgeAtype = new ArrayList<>();
    List<Long> edgeNodeId = new ArrayList<>();
    List<Integer> edgeAtype = new ArrayList<>();

    // getNodes(attr)
    List<Integer> warmupNodeAttrIds1 = new ArrayList<>();
    List<String> warmupNodeAttrs1 = new ArrayList<>();
    List<Integer> nodeAttrIds1 = new ArrayList<>();
    List<String> nodeAttrs1 = new ArrayList<>();
    // second set for getNodes(attr1, attr2)
    List<Integer> warmupNodeAttrIds2 = new ArrayList<>();
    List<String> warmupNodeAttrs2 = new ArrayList<>();
    List<Integer> nodeAttrIds2 = new ArrayList<>();
    List<String> nodeAttrs2 = new ArrayList<>();

    // assoc_range()
    List<Long> warmupAssocRangeNodes = new ArrayList<>();
    List<Long> assocRangeNodes = new ArrayList<>();
    List<Integer> warmupAssocRangeAtypes = new ArrayList<>();
    List<Integer> assocRangeAtypes = new ArrayList<>();
    List<Integer> warmupAssocRangeOffsets = new ArrayList<>();
    List<Integer> assocRangeOffsets = new ArrayList<>();
    List<Integer> warmupAssocRangeLengths = new ArrayList<>();
    List<Integer> assocRangeLengths = new ArrayList<>();

    // assoc_count()
    List<Long> warmupAssocCountNodes = new ArrayList<>();
    List<Long> assocCountNodes = new ArrayList<>();
    List<Integer> warmupAssocCountAtypes = new ArrayList<>();
    List<Integer> assocCountAtypes = new ArrayList<>();

    // obj_get()
    List<Long> warmupObjGetIds = new ArrayList<>();
    List<Long> objGetIds = new ArrayList<>();

    // assoc_get()
    List<Long> warmupAssocGetNodes = new ArrayList<>();
    List<Long> assocGetNodes = new ArrayList<>();
    List<Integer> warmupAssocGetAtypes = new ArrayList<>();
    List<Integer> assocGetAtypes = new ArrayList<>();
    List<Set<Long>> warmupAssocGetDstIdSets = new ArrayList<>();
    List<Set<Long>> assocGetDstIdSets = new ArrayList<>();
    List<Long> warmupAssocGetTimeLows = new ArrayList<>();
    List<Long> assocGetTimeLows = new ArrayList<>();
    List<Long> warmupAssocGetTimeHighs = new ArrayList<>();
    List<Long> assocGetTimeHighs = new ArrayList<>();

    // assoc_time_range()
    List<Long> warmupAssocTimeRangeNodes = new ArrayList<>();
    List<Long> assocTimeRangeNodes = new ArrayList<>();
    List<Integer> warmupAssocTimeRangeAtypes = new ArrayList<>();
    List<Integer> assocTimeRangeAtypes = new ArrayList<>();
    List<Long> warmupAssocTimeRangeTimeLows = new ArrayList<>();
    List<Long> assocTimeRangeTimeLows = new ArrayList<>();
    List<Long> warmupAssocTimeRangeTimeHighs = new ArrayList<>();
    List<Long> assocTimeRangeTimeHighs = new ArrayList<>();
    List<Integer> warmupAssocTimeRangeLimits = new ArrayList<>();
    List<Integer> assocTimeRangeLimits = new ArrayList<>();


    public static void main(String[] args) throws Exception {
        benchClassName = args[0];
        String latencyOrThroughput = args[1];
        name = args[2];
        queryPath = args[3];
        outputPath = args[4];
        int numClients = Integer.parseInt(args[5]);
        WARMUP_N = Integer.parseInt(args[6]);
        MEASURE_N = Integer.parseInt(args[7]);

        String fullClassName = Benchmark.class.getPackage().getName() + "." + benchClassName;
        Benchmark b = (Benchmark) Class.forName(fullClassName).newInstance();
        b.readQueries();
        if ("latency".equals(latencyOrThroughput)) {
            if (System.getenv("BENCH_PRINT_RESULTS") != null) {
                resOut = makeFileWriter(benchClassName + ".titan_result", false);
                System.out.println("Logging results to " + benchClassName + ".titan_result");
            }
            b.benchLatency();
        } else if ("throughput".equals(latencyOrThroughput)) {
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
                    if (resList.get(0) instanceof Long) {
                        List<Long> resLongList = (List<Long>) resList;
                        Collections.sort(resLongList);
                        print(resLongList, resOut);
                    } else {
                        print(resList, resOut);
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

    public static <T> T modGet(List<T> xs, int i) {
        return xs.get(i % xs.size());
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

    static void getLong(String file, List<Long> neighbors) {
        try {
            BufferedReader br = new BufferedReader(new FileReader(queryPath + "/" + file));
            List<String> lines = new ArrayList<>();
            String line = br.readLine();
            while (line != null) {
                lines.add(line);
                line = br.readLine();
            }
            for (int i = 0; i < lines.size(); i++) {
                neighbors.add(Long.parseLong(lines.get(i)));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static void getLongInteger(String file, List<Long> nodeIds, List<Integer> atypes) {
        try {
            BufferedReader br = new BufferedReader(new FileReader(queryPath + "/" + file));
            String line = br.readLine();
            while (line != null) {
                String[] toks = line.split(",");
                nodeIds.add(Long.valueOf(toks[0]));
                atypes.add(Integer.valueOf(toks[1]));
                line = br.readLine();
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
