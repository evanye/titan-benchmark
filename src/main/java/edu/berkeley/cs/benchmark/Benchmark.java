package edu.berkeley.cs.benchmark;

import edu.berkeley.cs.Graph;

import java.io.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Benchmark {

    private static int WARMUP_N;
    private static int MEASURE_N;

    // get_nhbrs(n)
    static List<Long> warmupNhbrs = new ArrayList<>();
    static List<Long> nhbrs = new ArrayList<>();

    // get_nhbrs(n, attr)
    static List<Long> warmupNhbrNodeIds = new ArrayList<>();
    static List<Integer> warmupNhbrNodeAttrIds = new ArrayList<>();
    static List<String> warmupNhbrNodeAttrs = new ArrayList<>();
    static List<Long> nhbrNodeIds = new ArrayList<>();
    static List<Integer> nhbrNodeAttrIds = new ArrayList<>();
    static List<String> nhbrNodeAttrs = new ArrayList<>();

    // get_nhbrs(n, atype)
    static List<Long> warmupNhbrAtypeIds = new ArrayList<>();
    static List<Long> warmupNhbrAtypeAtypes = new ArrayList<>();
    static List<Long> nhbrAtypeIds = new ArrayList<>();
    static List<Long> nhbrAtypeAtypes = new ArrayList<>();

    // get_nodes(attr) and get_nodes(attr1, attr2)
    static List<Integer> warmupNodeAttrIds1 = new ArrayList<Integer>();
    static List<String> warmupNodeAttrs1 = new ArrayList<String>();
    static List<Integer> warmupNodeAttrIds2 = new ArrayList<Integer>();
    static List<String> warmupNodeAttrs2 = new ArrayList<String>();
    static List<Integer> nodeAttrIds1 = new ArrayList<Integer>();
    static List<String> nodeAttrs1 = new ArrayList<String>();
    static List<Integer> nodeAttrIds2 = new ArrayList<Integer>();
    static List<String> nodeAttrs2 = new ArrayList<String>();

    public static <T> T modGet(List<T> xs, int i) {
        return xs.get(i % xs.size());
    }

    public static <T> void print(String header, List<T> xs, PrintWriter out) {
        out.println(header);
        for (T x : xs) {
            out.printf("%s ", x);
        }
        out.println();
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

    public static void getNodeQueries(
            String queryPath, List<Integer> warmupAttr1, List<Integer> warmupAttr2,
            List<String> warmupQuery1, List<String> warmupQuery2,
            List<Integer> attr1, List<Integer> attr2,
            List<String> query1, List<String> query2) {

        getNodeQueries(queryPath + "/node_warmup_100000.txt", warmupAttr1, warmupAttr2, warmupQuery1, warmupQuery2);
        getNodeQueries(queryPath + "/node_query_100000.txt", attr1, attr2, query1, query2);
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

    public static void getNeighborQueries(String queryPath, List<Long> warmupNeighbors, List<Long> neighbors) {
        getNeighborQueries(queryPath + "/neighbor_warmup_100000.txt", warmupNeighbors);
        getNeighborQueries(queryPath + "/neighbor_query_100000.txt", neighbors);
    }

    public static void getNeighborQueries(String file, List<Long> nhbrs) {
        try {
            BufferedReader br = new BufferedReader(new FileReader(file));
            List<String> lines = new ArrayList<>();
            String line = br.readLine();
            while (line != null) {
                lines.add(line);
                line = br.readLine();
            }
            for (int i = 0; i < lines.size(); i++) {
                nhbrs.add(Long.parseLong(lines.get(i)));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void getNeighborAtypeQueries(
            String file, List<Long> nodeIds, List<Long> atypes) {

        try {
            BufferedReader br = new BufferedReader(new FileReader(file));
            String line = br.readLine();
            while (line != null) {
                String[] toks = line.split(",");
                nodeIds.add(Long.valueOf(toks[0]));
                atypes.add(Long.valueOf(toks[1]));
                line = br.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void getNeighborNodeQueries(
            String query_path, List<Long> warmup_neighbor_indices, List<Long> neighbor_indices,
            List<Integer> warmup_node_attributes, List<Integer> node_attributes,
            List<String> warmup_node_queries, List<String> node_queries) {
        getNeighborNodeQueries(query_path + "/neighbor_node_warmup_100000.txt",
                warmup_neighbor_indices, warmup_node_attributes, warmup_node_queries);
        getNeighborNodeQueries(query_path + "/neighbor_node_query_100000.txt",
                neighbor_indices, node_attributes, node_queries);
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

    public static PrintWriter makeFileWriter(String output_file) {
        try {
            return new PrintWriter(new BufferedWriter(new FileWriter(output_file)));
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}
