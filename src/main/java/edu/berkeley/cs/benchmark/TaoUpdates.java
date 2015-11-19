package edu.berkeley.cs.benchmark;

import edu.berkeley.cs.titan.Graph;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Random;

public class TaoUpdates extends Benchmark<Object> {

    // Twitter
    final static int NUM_NODES = 41652230;
    final static int NUM_ATYPES = 5;
    public final static long MAX_TIME = 1441905687237L;
    public static String ATTR_FOR_NEW_EDGES;
    {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 128; ++i) {
            sb.append('|');
        }
        ATTR_FOR_NEW_EDGES = sb.toString();
    }

    final static int SEED = 1618;
    final static Random rand = new Random(SEED);

    @Override
    public void readQueries() {
    }

    @Override
    public void benchLatency() {
        Graph graph = new Graph();
        PrintWriter assocAddOut = makeFileWriter("taoUpdates.csv", false);
        PrintWriter assocToDelete = makeFileWriter("updates.csv", false);
        Random rand = new Random(SEED);

        System.out.println("Titan tao update query latency");
        System.out.println("Warming up for " + WARMUP_N + " queries");
        int ret;
        long start, end;

        int src, atype, dst;

        int edgesAdded = 0, edgesRemoved = 0;

        for (int i = 0; i < WARMUP_N; i++) {
            graph.restartTransaction();

            src = rand.nextInt(NUM_NODES);
            atype = rand.nextInt(NUM_ATYPES);
            dst = rand.nextInt(NUM_NODES);

            assocToDelete.println(src + "," + atype + "," + dst);
            graph.assocAdd(src, atype, dst, MAX_TIME, ATTR_FOR_NEW_EDGES);
            edgesAdded++;
        }

        System.out.println("Measuring for " + MEASURE_N + " queries");
        for (int i = 0; i < MEASURE_N; i++) {
            graph.restartTransaction();

            src = rand.nextInt(NUM_NODES);
            atype = rand.nextInt(NUM_ATYPES);
            dst = rand.nextInt(NUM_NODES);

            start = System.nanoTime();
            ret = graph.assocAdd(src, atype, dst, MAX_TIME, ATTR_FOR_NEW_EDGES);
            end = System.nanoTime();

            assocToDelete.println(src + "," + atype + "," + dst);
            assocAddOut.println(ret + "," + (end - start) * 1. / 1e3);
            edgesAdded++;
        }

        assocAddOut.close();
        assocToDelete.close();
        Benchmark.printMemoryFootprint();

        System.out.println("Removing added edges");
        try {
            BufferedReader br = new BufferedReader(new FileReader(outputPath + "/" + "updates.csv"));
            String line = br.readLine();
            while (line != null) {
                String[] toks = line.split(",");
                src = Integer.parseInt(toks[0]);
                atype = Integer.parseInt(toks[1]);
                dst = Integer.parseInt(toks[2]);

                boolean res = graph.assocDelete(src, atype, dst);
                if (res) edgesRemoved++;
                line = br.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("Added :" + edgesAdded + " edges and removed: " + edgesRemoved + " edges");
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
