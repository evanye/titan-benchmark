package edu.berkeley.cs.benchmark;

import edu.berkeley.cs.titan.Graph;

import java.util.Random;

public abstract class RunThroughput implements Runnable {
    public static final long SEED = 1945L;

    int clientId;
    protected Random rand;
    protected Graph g;
    // holds the results of the benchmarking
    private volatile double queryThroughput;
    private volatile double resultThroughput;

    public RunThroughput(int clientId) {
        this.clientId = clientId;
        rand = new Random(SEED + clientId);
        g = new Graph();
    }

    public abstract void warmupQuery();
    public abstract int query();

    @Override
    public void run() {
        // warmup
        int i = 0;
        long warmupStart = System.nanoTime();
        System.out.println("Client " + clientId + " warming up for " + (Benchmark.WARMUP_TIME / 1E9) + " seconds.");
        while (System.nanoTime() - warmupStart < Benchmark.WARMUP_TIME) {
            if (i % 10000 == 0) {
                g.restartTransaction();
            }
            warmupQuery();
            ++i;
        }
        System.out.println("Client " + clientId + " finished warming up!");

        // measure
        i = 0;
        long results = 0;
        System.out.println("Client " + clientId + " measuring for " + (Benchmark.MEASURE_TIME / 1E9) + " seconds.");
        long start = System.nanoTime();
        while (System.nanoTime() - start < Benchmark.MEASURE_TIME) {
            if (i % 10000 == 0) {
                g.restartTransaction();
            }

            results += query();
            ++i;
        }
        long end = System.nanoTime();
        g.restartTransaction();

        System.out.println("Client " + clientId + " finished measuring!");
        double totalSeconds = (end - start) * 1. / 1e9;
        queryThroughput = ((double) i) / totalSeconds;
        resultThroughput = ((double) results ) / totalSeconds;

        System.out.println("Client " + clientId + " cooling down for " + (Benchmark.COOLDOWN_TIME / 1E9) + " seconds.");
        long cooldownStart = System.nanoTime();
        while (System.nanoTime() - cooldownStart < Benchmark.COOLDOWN_TIME) {
            warmupQuery();
        }
        System.out.println("Client " + clientId + " finished cooling down!");
    }

    public double getQueryThroughput() {
        return queryThroughput;
    }

    public double getResultThroughput() {
        return resultThroughput;
    }
}

