package edu.berkeley.cs.benchmark.tao;

import edu.berkeley.cs.benchmark.Benchmark;
import edu.berkeley.cs.titan.Graph;

import java.io.*;
import java.nio.file.Paths;
import java.util.*;

public abstract class BenchTao {

    public static int WARMUP_N;
    public static int MEASURE_N;

    public static final long WARMUP_TIME = (long) (60 * 1e9); // 60 seconds
    public static final long MEASURE_TIME = (long) (120 * 1e9);

    Graph g;
    String queryPath;
    String outputPath;
    static String benchClassName;

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

    PrintWriter throughputOut;

    public static void main(String[] args) throws Exception {
        benchClassName = args[0];
        String latencyOrThroughput = args[1];
        String name = args[2];
        String queryPath = args[3];
        String outputPath = args[4];
        int WARMUP_N = Integer.parseInt(args[5]);
        int MEASURE_N = Integer.parseInt(args[6]);

        String fullClassName = BenchTao.class.getPackage().getName() + "." + benchClassName;
        BenchTao b = (BenchTao) Class.forName(fullClassName).newInstance();
        b.init(name, queryPath, outputPath, WARMUP_N, MEASURE_N);
        b.readQueries();
        if ("latency".equals(latencyOrThroughput)) {
            b.benchLatency();
        } else if ("throughput".equals(latencyOrThroughput)) {
            b.benchThroughput();
        } else {
            System.err.println("Please choose 'latency' or 'throughput'.");
        }
        System.exit(0);
    }

    public void init(String titanName, String queryPath, String outputPath, int WARMUP_N, int MEASURE_N) {
        g = new Graph(titanName);
        this.queryPath = queryPath; this.outputPath = outputPath;
        this.WARMUP_N = WARMUP_N; this.MEASURE_N = MEASURE_N;
        try {
            throughputOut = new PrintWriter(new FileWriter(
                    Paths.get(outputPath, g.getName() + "_throughput.csv").toFile(), true));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public abstract void readQueries();
    public abstract int warmupQuery(int i);
    public abstract int query(int i);

    public void benchLatency() {
        PrintWriter out = makeFileWriter(g.getName() + "_tao_" + benchClassName + ".csv");
        System.out.println("Titan " + benchClassName + " tao query latency");
        //fullWarmup(g);
        System.out.println("Warming up for " + WARMUP_N + " queries");
        for (int i = 0; i < WARMUP_N; i++) {
            if (i % 10000 == 0) {
                g.restartTransaction();
                System.out.println("Warmed up for " + i + " queries");
            }
            warmupQuery(i);
        }

        System.out.println("Measuring for " + MEASURE_N + " queries");
        for (int i = 0; i < MEASURE_N; i++) {
            if (i % 10000 == 0) {
                g.restartTransaction();
                System.out.println("Measured for " + i + " queries");
            }
            long start = System.nanoTime();
            int numResults = query(i);
            long end = System.nanoTime();
            double microsecs = (end - start) / ((double) 1000);
            out.println(numResults + "," + microsecs);
        }
        out.close();
        Benchmark.printMemoryFootprint();
    }

    public void benchThroughput() {
        System.out.println("Titan " + benchClassName + " tao query throughput");
        System.out.println("Warming up for " + WARMUP_TIME / 1E9 + " seconds");
        int i = 0;
        long warmupStart = System.nanoTime();
        while (System.nanoTime() - warmupStart < WARMUP_TIME) {
            if (i % 10000 == 0) {
                g.restartTransaction();
                System.out.println("Warmed up for " + i + " queries");
            }
            warmupQuery(i);
            ++i;
        }

        System.out.println("Measuring for " + MEASURE_TIME/ 1E9 + " seconds");

        i = 0;
        long numResults = 0;
        long start = System.nanoTime();
        while (System.nanoTime() - start < MEASURE_TIME) {
            if (i % 10000 == 0) {
                g.restartTransaction();
            }
            numResults += query(i);
            ++i;
        }
        double totalSeconds = (System.nanoTime() - start) * 1. / 1e9;
        double queryThroughput = ((double) i) / totalSeconds;
        double resultThroughput = ((double) numResults) / totalSeconds;
        throughputOut.println(benchClassName + " throughput - qps: " + queryThroughput + " edges/second: " + resultThroughput);
        throughputOut.close();
        Benchmark.printMemoryFootprint();
    }

    public static <T> T modGet(List<T> xs, int i) {
        return xs.get(i % xs.size());
    }

    public static void readAssocCountQueries(String file, List<Long> nodes, List<Integer> atypes) {
        Benchmark.getNeighborAtypeQueries(file, nodes, atypes);
    }

    public static void readAssocRangeQueries(
            String file, List<Long> nodes, List<Integer> atypes,
            List<Integer> offsets, List<Integer> lengths) {

        try {
            BufferedReader br = new BufferedReader(new FileReader(file));
            String line = br.readLine();
            while (line != null) {
                int idx = line.indexOf(',');
                nodes.add(Long.parseLong(line.substring(0, idx)));

                int idx2 = line.indexOf(',', idx + 1);
                atypes.add(Integer.parseInt(line.substring(idx + 1, idx2)));

                int idx3 = line.indexOf(',', idx2 + 1);
                offsets.add(Integer.parseInt(line.substring(idx2 + 1, idx3)));

                lengths.add(Integer.parseInt(line.substring(idx3 + 1)));
                line = br.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void readAssocGetQueries(
            String file, List<Long> nodes, List<Integer> atypes,
            List<Set<Long>> dstIdSets, List<Long> tLows, List<Long> tHighs) {

        try {
            BufferedReader br = new BufferedReader(new FileReader(file));
            String line = br.readLine();
            while (line != null) {
                int idx = line.indexOf(',');
                nodes.add(Long.parseLong(line.substring(0, idx)));

                int idx2 = line.indexOf(',', idx + 1);
                atypes.add(Integer.parseInt(line.substring(idx + 1, idx2)));

                int idx3 = line.indexOf(',', idx2 + 1);
                tLows.add(Long.parseLong(line.substring(idx2 + 1, idx3)));

                int idx4 = line.indexOf(',', idx3 + 1);
                tHighs.add(Long.parseLong(line.substring(idx3 + 1, idx4)));

                int idxLast = idx4, idxCurr;
                Set<Long> dstIdSet = new HashSet<>();
                while (true) {
                    idxCurr = line.indexOf(',', idxLast + 1);
                    if (idxCurr == -1) {
                        break;
                    }
                    dstIdSet.add(Long.parseLong(
                            line.substring(idxLast + 1, idxCurr)));
                    idxLast = idxCurr;
                }
                dstIdSet.add(Long.parseLong(line.substring(idxLast + 1)));
                dstIdSets.add(dstIdSet);
                line = br.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void readAssocTimeRangeQueries(
            String file, List<Long> nodes, List<Integer> atypes,
            List<Long> tLows, List<Long> tHighs, List<Integer> limits) {

        try {
            BufferedReader br = new BufferedReader(new FileReader(file));
            String line = br.readLine();
            while (line != null) {
                int idx = line.indexOf(',');
                nodes.add(Long.parseLong(line.substring(0, idx)));

                int idx2 = line.indexOf(',', idx + 1);
                atypes.add(Integer.parseInt(line.substring(idx + 1, idx2)));

                int idx3 = line.indexOf(',', idx2 + 1);
                tLows.add(Long.parseLong(line.substring(idx2 + 1, idx3)));

                int idx4 = line.indexOf(',', idx3 + 1);
                tHighs.add(Long.parseLong(line.substring(idx3 + 1, idx4)));

                limits.add(Integer.parseInt(line.substring(idx4 + 1)));

                line = br.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public PrintWriter makeFileWriter(String outputName) {
        try {
            return new PrintWriter(new BufferedWriter(
                    new FileWriter(Paths.get(outputPath, outputName).toFile())));
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}
