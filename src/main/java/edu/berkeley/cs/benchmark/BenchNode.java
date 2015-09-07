package edu.berkeley.cs.benchmark;

import edu.berkeley.cs.Graph;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import static edu.berkeley.cs.benchmark.BenchUtils.getNodeQueries;
import static edu.berkeley.cs.benchmark.BenchUtils.makeFileWriter;

public class BenchNode {
    private static int WARMUP_N = 100000;
    private static int MEASURE_N = 100000;

    public static void main(String[] args) {
        String type = args[0];
        String db_path = args[1];
        String output_file = args[2];
        WARMUP_N = Integer.parseInt(args[3]);
        MEASURE_N = Integer.parseInt(args[4]);
        String query_path = args[5];

        Graph g = new Graph();
        List<Integer> warmupAttributes1 = new ArrayList<Integer>();
        List<Integer> warmupAttributes2 = new ArrayList<Integer>();
        List<String> warmupQueries1 = new ArrayList<String>();
        List<String> warmupQueries2 = new ArrayList<String>();
        List<Integer> attributes1 = new ArrayList<Integer>();
        List<Integer> attributes2 = new ArrayList<Integer>();
        List<String> queries1 = new ArrayList<String>();
        List<String> queries2 = new ArrayList<String>();
        PrintWriter out = makeFileWriter(output_file);

        getNodeQueries(query_path, warmupAttributes1,
                warmupAttributes2, warmupQueries1, warmupQueries2,
                attributes1, attributes2, queries1, queries2);

        if (type.equals("node-throughput")) {
//            nodeThroughput(db_path, warmupAttributes1, warmupQueries1,
//                    attributes1, queries1, output_file);
        } else if (type.equals("node-latency")) {
            nodeLatency(g, out, warmupAttributes1,
                    warmupQueries1, attributes1, queries1);
        } else if (type.equals("node-node-latency")) {
//            nodeNodeLatency(db_path, neo4jPageCacheMemory, warmupAttributes1,
//                    warmupAttributes2, warmupQueries1, warmupQueries2,
//                    attributes1, attributes2, queries1, queries2, output_file);
        } else {
            System.out.println("No type " + type + " is supported!");
        }
    }

    private static void nodeLatency(Graph g, PrintWriter out, List<Integer> warmupAttributes, List<String> warmupQueries, List<Integer> attributes, List<String> queries) {
    }
}
