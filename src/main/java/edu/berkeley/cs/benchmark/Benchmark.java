package edu.berkeley.cs.benchmark;

import edu.berkeley.cs.Graph;

import java.io.*;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public abstract class Benchmark {

    int WARMUP_N;
    int MEASURE_N;

    Graph g;
    String queryPath;
    String outputPath;

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


    public static void main(String[] args) {
        String benchClassName = args[0];
        String latencyOrThroughput = args[1];
        String name = args[2];
        String queryPath = args[3];
        String outputPath = args[4];
        int WARMUP_N = Integer.parseInt(args[5]);
        int MEASURE_N = Integer.parseInt(args[6]);

        Class<Benchmark> c = null; //Benchmark.class.getPackage() + "." + benchClassName;
        try {
            c = (Class<Benchmark>) Class.forName(benchClassName);
            Benchmark b = c.newInstance();
            b.init(name, queryPath, outputPath, WARMUP_N, MEASURE_N);
            b.readQueries();
            if ("latency".equals(latencyOrThroughput)) {
                b.benchLatency();
            } else if ("throughput".equals(latencyOrThroughput)) {
                b.benchThroughput();
            } else {
                System.err.println("Please choose 'latency' or 'throughput'.");
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
    }

    public void init(String titanName, String queryPath, String outputPath, int WARMUP_N, int MEASURE_N) {
        g = new Graph(titanName);
        this.queryPath = queryPath; this.outputPath = outputPath;
        this.WARMUP_N = WARMUP_N; this.MEASURE_N = MEASURE_N;
    }

    public abstract void readQueries();

    public abstract void benchLatency();
    public abstract void benchThroughput();

    public static <T> T modGet(List<T> xs, int i) {
        return xs.get(i % xs.size());
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

    public static void getNeighborQueries(String file, List<Long> neighbors) {
        try {
            BufferedReader br = new BufferedReader(new FileReader(file));
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

    public static void getNodeQueries(
            String file, List<Integer> indices1, List<Integer> indices2,
            List<String> queries1, List<String> queries2) {

        try {
            BufferedReader br = new BufferedReader(new FileReader(file));
            String line = br.readLine();
            while (line != null) {
                String[] tokens = line.split("\\x02");
                indices1.add(Integer.parseInt(tokens[0]));
                queries1.add(tokens[1]);
                indices2.add(Integer.parseInt(tokens[2]));
                queries2.add(tokens[3]);
                line = br.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void getNeighborAtypeQueries(
            String file, List<Long> nodeIds, List<Integer> atypes) {

        try {
            BufferedReader br = new BufferedReader(new FileReader(file));
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

    public static void getNeighborNodeQueries(
            String file, List<Long> indices,
            List<Integer> attributes, List<String> queries) {

        try {
            BufferedReader br = new BufferedReader(new FileReader(file));
            String line = br.readLine();
            while (line != null) {
                int idx = line.indexOf(',');
                indices.add(Long.parseLong(line.substring(0, idx)));
                int idx2 = line.indexOf(',', idx + 1);
                attributes.add(Integer.parseInt(line.substring(idx + 1, idx2)));
                queries.add(line.substring(idx2 + 1));
                line = br.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void readAssocRangeQueries(
            String file, List<Long> nodes, List<Long> atypes,
            List<Integer> offsets, List<Integer> lengths) {

        try {
            BufferedReader br = new BufferedReader(new FileReader(file));
            String line = br.readLine();
            while (line != null) {
                int idx = line.indexOf(',');
                nodes.add(Long.parseLong(line.substring(0, idx)));

                int idx2 = line.indexOf(',', idx + 1);
                atypes.add(Long.parseLong(line.substring(idx + 1, idx2)));

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
            String file, List<Long> nodes, List<Long> atypes,
            List<Set<Long>> dstIdSets, List<Long> tLows, List<Long> tHighs) {

        try {
            BufferedReader br = new BufferedReader(new FileReader(file));
            String line = br.readLine();
            while (line != null) {
                int idx = line.indexOf(',');
                nodes.add(Long.parseLong(line.substring(0, idx)));

                int idx2 = line.indexOf(',', idx + 1);
                atypes.add(Long.parseLong(line.substring(idx + 1, idx2)));

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
            String file, List<Long> nodes, List<Long> atypes,
            List<Long> tLows, List<Long> tHighs, List<Integer> limits) {

        try {
            BufferedReader br = new BufferedReader(new FileReader(file));
            String line = br.readLine();
            while (line != null) {
                int idx = line.indexOf(',');
                nodes.add(Long.parseLong(line.substring(0, idx)));

                int idx2 = line.indexOf(',', idx + 1);
                atypes.add(Long.parseLong(line.substring(idx + 1, idx2)));

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
