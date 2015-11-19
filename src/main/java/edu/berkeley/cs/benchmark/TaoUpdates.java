package edu.berkeley.cs.benchmark;

import edu.berkeley.cs.titan.Graph;

import java.io.PrintWriter;
import java.util.Random;

public class TaoUpdates extends Benchmark<Object> {

    // FIXME: hard-coded
    final static int MAX_NUM_NEW_EDGES = 200000;

    // Twitter
    final static int NUM_NODES = 41652230;
    final static int NUM_ATYPES = 5;
    final static long MAX_TIME = 1441905687237L;
    static String ATTR_FOR_NEW_EDGES;
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
        Random rand = new Random(SEED);

        WARMUP_N = MAX_NUM_NEW_EDGES / 10;
        MEASURE_N = MAX_NUM_NEW_EDGES - WARMUP_N;

        System.out.println("Titan tao update query latency");
        System.out.println("Warming up for " + WARMUP_N + " queries");
        int ret;
        long start, end;

        int src, atype, dst;

        for (int i = 0; i < WARMUP_N; i++) {
            graph.restartTransaction();

            src = rand.nextInt(NUM_NODES);
            atype = rand.nextInt(NUM_ATYPES);
            dst = rand.nextInt(NUM_NODES);

            graph.assocAdd(src, atype, dst, MAX_TIME, ATTR_FOR_NEW_EDGES);
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

            assocAddOut.println(ret + "," + (end - start) * 1. / 1e3);
        }

        assocAddOut.close();
        Benchmark.printMemoryFootprint();
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
