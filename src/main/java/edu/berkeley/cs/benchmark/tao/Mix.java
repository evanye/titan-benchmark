package edu.berkeley.cs.benchmark.tao;

import edu.berkeley.cs.benchmark.Benchmark;
import edu.berkeley.cs.titan.Assoc;

import java.io.PrintWriter;
import java.util.List;
import java.util.Random;

public class Mix extends BenchTao {
    // Read workload distribution; from ATC 13 Bronson et al.
    final static double ASSOC_RANGE_PERC = 0.409;
    final static double OBJ_GET_PERC = 0.289;
    final static double ASSOC_GET_PERC = 0.157;
    final static double ASSOC_COUNT_PERC = 0.117;
    final static double ASSOC_TIME_RANGE_PERC = 0.028;

    final static int SEED = 1618;
    final static Random rand = new Random(SEED);

    @Override
    public void readQueries() {
        // assoc_range()
        readAssocRangeQueries(queryPath + "/assocRange_warmup.txt",
                warmupAssocRangeNodes, warmupAssocRangeAtypes,
                warmupAssocRangeOffsets, warmupAssocRangeLengths);
        readAssocRangeQueries(queryPath + "assocRange_query.txt",
                assocRangeNodes, assocRangeAtypes,
                assocRangeOffsets, assocRangeLengths);

        // assoc_count()
        readAssocCountQueries(queryPath + "/assocCount_warmup.txt",
                warmupAssocCountNodes, warmupAssocCountAtypes);

        readAssocCountQueries(queryPath + "/assocCount_query.txt",
                assocCountNodes, assocCountAtypes);

        // obj_get()
        Benchmark.getNeighborQueries(queryPath + "/objGet_warmup.txt", warmupObjGetIds);
        Benchmark.getNeighborQueries(queryPath + "/objGet_query.txt", objGetIds);

        // assoc_get()
        readAssocGetQueries(queryPath + "/assocGet_warmup.txt",
                warmupAssocGetNodes, warmupAssocGetAtypes,
                warmupAssocGetDstIdSets, warmupAssocGetTimeLows,
                warmupAssocGetTimeHighs);

        readAssocGetQueries(queryPath + "/assocGet_query.txt",
                assocGetNodes, assocGetAtypes,
                assocGetDstIdSets, assocGetTimeLows, assocGetTimeHighs);

        // assoc_time_range()
        readAssocTimeRangeQueries(queryPath + "/assocTimeRange_warmup.txt",
                warmupAssocTimeRangeNodes,
                warmupAssocTimeRangeAtypes, warmupAssocTimeRangeTimeLows,
                warmupAssocTimeRangeTimeHighs, warmupAssocTimeRangeLimits);

        readAssocTimeRangeQueries(queryPath + "/assocTimeRange_query.txt",
                assocTimeRangeNodes, assocTimeRangeAtypes,
                assocTimeRangeTimeLows, assocTimeRangeTimeHighs,
                assocTimeRangeLimits);
    }

    public static int chooseQuery(Random rand) {
        double d = rand.nextDouble();
        if (d < ASSOC_RANGE_PERC) {
            return 0;
        } else if (d < ASSOC_RANGE_PERC + OBJ_GET_PERC) {
            return 1;
        } else if (d < ASSOC_RANGE_PERC + OBJ_GET_PERC + ASSOC_GET_PERC) {
            return 2;
        } else if (d < ASSOC_RANGE_PERC + OBJ_GET_PERC +
                ASSOC_GET_PERC + ASSOC_COUNT_PERC) {

            return 3;
        }
        return 4;
    }

    int dispatchMixQueryWarmup(Random rand) {
        int i;
        switch (chooseQuery(rand)) {
            case 0:
                // assoc_range
                i = rand.nextInt(warmupAssocRangeNodes.size());
                return g.assocRange(
                        modGet(warmupAssocRangeNodes, i),
                        modGet(warmupAssocRangeAtypes, i),
                        modGet(warmupAssocRangeOffsets, i),
                        modGet(warmupAssocRangeLengths, i)).size();
            case 1:
                // obj_get
                i = rand.nextInt(warmupObjGetIds.size());
                return g.objGet(modGet(warmupObjGetIds, i)).size();
            case 2:
                // assoc_get
                i = rand.nextInt(warmupAssocCountNodes.size());
                return g.assocGet(
                        modGet(warmupAssocGetNodes, i),
                        modGet(warmupAssocGetAtypes, i),
                        modGet(warmupAssocGetDstIdSets, i),
                        modGet(warmupAssocGetTimeLows, i),
                        modGet(warmupAssocGetTimeHighs, i)).size();
            case 3:
                // assoc_count
                i = rand.nextInt(warmupAssocCountNodes.size());
                g.assocCount(
                        modGet(warmupAssocCountNodes, i),
                        modGet(warmupAssocCountAtypes, i));
                return 1;
            case 4:
                // assoc_time_range
                i = rand.nextInt(warmupAssocTimeRangeNodes.size());
                return g.assocTimeRange(
                        modGet(warmupAssocTimeRangeNodes, i),
                        modGet(warmupAssocTimeRangeAtypes, i),
                        modGet(warmupAssocTimeRangeTimeLows, i),
                        modGet(warmupAssocTimeRangeTimeHighs, i),
                        modGet(warmupAssocTimeRangeLimits, i)).size();
        }
        return 0;
    }

    int dispatchMixQuery(Random rand) {
        int i;
        switch (chooseQuery(rand)) {
            case 0:
                // assoc_range
                i = rand.nextInt(assocRangeNodes.size());
                return g.assocRange(
                        modGet(assocRangeNodes, i),
                        modGet(assocRangeAtypes, i),
                        modGet(assocRangeOffsets, i),
                        modGet(assocRangeLengths, i)).size();
            case 1:
                // obj_get
                i = rand.nextInt(objGetIds.size());
                return g.objGet(modGet(objGetIds, i)).size();
            case 2:
                // assoc_get
                i = rand.nextInt(assocGetNodes.size());
                return g.assocGet(
                        modGet(assocGetNodes, i),
                        modGet(assocGetAtypes, i),
                        modGet(assocGetDstIdSets, i),
                        modGet(assocGetTimeLows, i),
                        modGet(assocGetTimeHighs, i)).size();
            case 3:
                // assoc_count
                i = rand.nextInt(assocCountNodes.size());
                g.assocCount(
                        modGet(assocCountNodes, i),
                        modGet(assocCountAtypes, i));
                return 1;
            case 4:
                // assoc_time_range
                i = rand.nextInt(assocTimeRangeNodes.size());
                return g.assocTimeRange(
                        modGet(assocTimeRangeNodes, i),
                        modGet(assocTimeRangeAtypes, i),
                        modGet(assocTimeRangeTimeLows, i),
                        modGet(assocTimeRangeTimeHighs, i),
                        modGet(assocTimeRangeLimits, i)).size();
        }
        return 0;
    }

    @Override
    public void benchLatency() {
        PrintWriter assocRangeOut = makeFileWriter(g.getName() + "_mix_assocRangecsv");
        PrintWriter objGetOut = makeFileWriter(g.getName() + "_mix_objGet.csv");
        PrintWriter assocGetOut = makeFileWriter(g.getName() + "_mix_assocGet.csv");
        PrintWriter assocCountOut = makeFileWriter(g.getName() + "_mix_assocCount.csv");
        PrintWriter assocTimeRangeOut = makeFileWriter(g.getName() + "_mix_assocTimeRange.csv");

        Random rand = new Random(SEED);

        System.out.println("Titan Mix tao query latency");
        System.out.println("Warming up for " + WARMUP_N + " queries");
        for (int i = 0; i < WARMUP_N; i++) {
            if (i % 10000 == 0) {
                g.restartTransaction();
                System.out.println("Warmed up for " + i + " queries");
            }

            dispatchMixQueryWarmup(rand);
        }

        rand.setSeed(SEED); // re-seed
        long start, end;

        System.out.println("Measuring for " + MEASURE_N + " queries");
        for (int i = 0; i < MEASURE_N; i++) {
            if (i % 10000 == 0) {
                g.restartTransaction();
                System.out.println("Measured for " + i + " queries");
            }
            List<Assoc> assocs;
            switch (chooseQuery(rand)) {
                case 0:
                    // assoc_range
                    start = System.nanoTime();
                    assocs = g.assocRange(
                            modGet(assocRangeNodes, i),
                            modGet(assocRangeAtypes, i),
                            modGet(assocRangeOffsets, i),
                            modGet(assocRangeLengths, i));
                    end = System.nanoTime();
                    assocRangeOut.println(
                            assocs.size() + "," + (end - start) / 1e3);
                    break;
                case 1:
                    // obj_get
                    start = System.nanoTime();
                    List<String> attrs = g.objGet(modGet(objGetIds, i));
                    end = System.nanoTime();
                    objGetOut.println(
                            attrs.size() + "," + (end - start) / 1e3);
                    break;
                case 2:
                    // assoc_get
                    start = System.nanoTime();
                    assocs = g.assocGet(
                            modGet(assocGetNodes, i),
                            modGet(assocGetAtypes, i),
                            modGet(assocGetDstIdSets, i),
                            modGet(assocGetTimeLows, i),
                            modGet(assocGetTimeHighs, i));
                    end = System.nanoTime();
                    assocGetOut.println(
                            assocs.size() + "," + (end - start) / 1e3);
                    break;
                case 3:
                    // assoc_count
                    start = System.nanoTime();
                    long count = g.assocCount(
                            modGet(assocCountNodes, i),
                            modGet(assocCountAtypes, i));
                    end = System.nanoTime();
                    assocCountOut.println(
                            count + "," + (end - start) / 1e3);
                    break;
                case 4:
                    // assoc_time_range
                    start = System.nanoTime();
                    assocs = g.assocTimeRange(
                            modGet(assocTimeRangeNodes, i),
                            modGet(assocTimeRangeAtypes, i),
                            modGet(assocTimeRangeTimeLows, i),
                            modGet(assocTimeRangeTimeHighs, i),
                            modGet(assocTimeRangeLimits, i));
                    end = System.nanoTime();
                    assocTimeRangeOut.println(
                            assocs.size() + "," + (end - start) / 1e3);
                    break;
            }
        }

        assocRangeOut.close();
        objGetOut.close();
        assocGetOut.close();
        assocCountOut.close();
        assocTimeRangeOut.close();
        Benchmark.printMemoryFootprint();
    }

    @Override
    public int warmupQuery(int i) {
        return dispatchMixQueryWarmup(rand);
    }

    @Override
    public int query(int i) {
        return dispatchMixQuery(rand);
    }
}
