package edu.berkeley.cs.benchmark;

import edu.berkeley.cs.Graph;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static edu.berkeley.cs.benchmark.BenchUtils.*;

public class BenchNeighborNode {
    private static int WARMUP_N;
    private static int MEASURE_N;

    public static void main(String[] args) {
        String type = args[0];
        String query_path = args[1];
        String output_file = args[2];
        WARMUP_N = Integer.parseInt(args[3]);
        MEASURE_N = Integer.parseInt(args[4]);

        Graph g = new Graph();
        PrintWriter out = makeFileWriter(output_file);
        List<Long> warmup_neighbor_indices = new ArrayList<Long>();
        List<Integer> warmup_node_attributes = new ArrayList<Integer>();
        List<String> warmup_node_queries = new ArrayList<String>();
        List<Long> neighbor_indices = new ArrayList<Long>();
        List<Integer> node_attributes = new ArrayList<Integer>();
        List<String> node_queries = new ArrayList<String>();
        BenchUtils.getNeighborNodeQueries(
                query_path, warmup_neighbor_indices, neighbor_indices,
                warmup_node_attributes, node_attributes,
                warmup_node_queries, node_queries);

        if (type.equals("latency")) {
            neighborNodeLatency(g, out, warmup_neighbor_indices, neighbor_indices,
                    warmup_node_attributes, node_attributes, warmup_node_queries, node_queries);
        } else {
            System.err.println("No type " + type + " is supported!");
        }
    }

    private static void neighborNodeLatency(
            Graph g, PrintWriter out,
            List<Long> warmup_neighbor_indices, List<Long> neighbor_indices,
            List<Integer> warmup_node_attributes, List<Integer> node_attributes,
            List<String> warmup_node_queries, List<String> node_queries) {

        System.out.println("Titan getNeighborNode query latency");
//        BenchUtils.fullWarmup(g);
        System.out.println("Warming up for " + WARMUP_N + " queries");
        for (int i = 0; i < WARMUP_N; i++) {
            if (i % 10000 == 0) {
                g.restartTransaction();
                System.out.println("Warmed up for " + i + " queries");
            }
            List<Long> nodes = g.getNeighborNode(modGet(warmup_neighbor_indices, i),
                    modGet(warmup_node_attributes, i), modGet(warmup_node_queries, i));
        }

        System.out.println("Measuring for " + MEASURE_N + " queries");
        for (int i = 0; i < MEASURE_N; i++) {
            if (i % 10000 == 0) {
                g.restartTransaction();
                System.out.println("Measured for " + i + " queries");
            }
            long start = System.nanoTime();
            List<Long> nodes = g.getNeighborNode(modGet(neighbor_indices, i),
                    modGet(node_attributes, i), modGet(node_queries, i));
            long end = System.nanoTime();
            double microsecs = (end - start) / ((double) 1000);
            out.println(nodes.size() + "," + microsecs);
        }
        out.close();
        printMemoryFootprint();
    }
}
