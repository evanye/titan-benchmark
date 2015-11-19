package edu.berkeley.cs.benchmark;

import edu.berkeley.cs.titan.Graph;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;

public class MixTaoWithUpdates extends Benchmark<Object> {
    // Read workload distribution; from ATC 13 Bronson et al.
    final static double UPDATE_RATE = 0.002;

    final static double ASSOC_RANGE_PERC = 0.409;
    final static double OBJ_GET_PERC = 0.289;
    final static double ASSOC_GET_PERC = 0.157;
    final static double ASSOC_COUNT_PERC = 0.117;
    final static double ASSOC_TIME_RANGE_PERC = 0.028;

    @Override
    public void readQueries() {
        // assoc_range()
        AssocRange.readAssocRangeQueries(AssocRange.WARMUP_FILE,
            warmupAssocRangeNodes, warmupAssocRangeAtypes,
            warmupAssocRangeOffsets, warmupAssocRangeLengths);
        AssocRange.readAssocRangeQueries(AssocRange.QUERY_FILE,
            assocRangeNodes, assocRangeAtypes,
            assocRangeOffsets, assocRangeLengths);

        // assoc_count()
        getLongInteger(AssocCount.WARMUP_FILE, warmupAssocCountNodes, warmupAssocCountAtypes);
        getLongInteger(AssocCount.QUERY_FILE, assocCountNodes, assocCountAtypes);

        // obj_get()
        getLong(ObjGet.WARMUP_FILE, warmupObjGetIds);
        getLong(ObjGet.QUERY_FILE, objGetIds);

        // assoc_get()
        AssocGet.readAssocGetQueries(AssocGet.WARMUP_FILE,
            warmupAssocGetNodes, warmupAssocGetAtypes,
            warmupAssocGetDstIdSets, warmupAssocGetTimeLows,
            warmupAssocGetTimeHighs);

        AssocGet.readAssocGetQueries(AssocGet.QUERY_FILE,
            assocGetNodes, assocGetAtypes,
            assocGetDstIdSets, assocGetTimeLows, assocGetTimeHighs);

        // assoc_time_range()
        AssocTimeRange.readAssocTimeRangeQueries(AssocTimeRange.WARMUP_FILE,
            warmupAssocTimeRangeNodes,
            warmupAssocTimeRangeAtypes, warmupAssocTimeRangeTimeLows,
            warmupAssocTimeRangeTimeHighs, warmupAssocTimeRangeLimits);

        AssocTimeRange.readAssocTimeRangeQueries(AssocTimeRange.QUERY_FILE,
            assocTimeRangeNodes, assocTimeRangeAtypes,
            assocTimeRangeTimeLows, assocTimeRangeTimeHighs,
            assocTimeRangeLimits);
    }

    public static int chooseQuery(Random rand) {
        double d = rand.nextDouble();
        if (d < UPDATE_RATE) {
            return 5; // assocAdd
        }
        d = rand.nextDouble();
        if (d < ASSOC_RANGE_PERC) {
            return 0;
        } else if (d < ASSOC_RANGE_PERC + OBJ_GET_PERC) {
            return 1;
        } else if (d < ASSOC_RANGE_PERC + OBJ_GET_PERC + ASSOC_GET_PERC) {
            return 2;
        } else if (d < ASSOC_RANGE_PERC + OBJ_GET_PERC +
            ASSOC_GET_PERC + ASSOC_COUNT_PERC) {
            return 3;
        } else {
            return 4;
        }
    }

    @Override
    public void benchLatency() {
    }

    @Override
    public RunThroughput getThroughputJob(int clientId) {
        return new RunThroughput(clientId) {
            @Override
            public void warmupQuery() {
                int i, src, atype, dst;
                switch (chooseQuery(rand)) {
                    case 0:
                        // assoc_range
                        i = rand.nextInt(assocRange_warmup);
                        g.assocRange(
                            warmupAssocRangeNodes[i],
                            warmupAssocRangeAtypes[i],
                            warmupAssocRangeOffsets[i],
                            warmupAssocRangeLengths[i]).size();
                    case 1:
                        // obj_get
                        i = rand.nextInt(objGet_warmup);
                        g.objGet(warmupObjGetIds[i]).size();
                    case 2:
                        // assoc_get
                        i = rand.nextInt(assocGet_warmup);
                        g.assocGet(
                            warmupAssocGetNodes[i],
                            warmupAssocGetAtypes[i],
//                                warmupAssocGetDstIdSets[i],
                            new HashSet(Arrays.asList(warmupAssocGetDstIdSets[i])),
                            warmupAssocGetTimeLows[i],
                            warmupAssocGetTimeHighs[i]).size();
                    case 3:
                        // assoc_count
                        i = rand.nextInt(assocCount_warmup);
                        g.assocCount(
                            warmupAssocCountNodes[i],
                            warmupAssocCountAtypes[i]);
                    case 4:
                        // assoc_time_range
                        i = rand.nextInt(assocTimeRange_warmup);
                        g.assocTimeRange(
                            warmupAssocTimeRangeNodes[i],
                            warmupAssocTimeRangeAtypes[i],
                            warmupAssocTimeRangeTimeLows[i],
                            warmupAssocTimeRangeTimeHighs[i],
                            warmupAssocTimeRangeLimits[i]).size();
                    case 5:
                        // assoc_add
                        src = rand.nextInt(TaoUpdates.NUM_NODES);
                        atype = rand.nextInt(TaoUpdates.NUM_ATYPES);
                        dst = rand.nextInt(TaoUpdates.NUM_NODES);

                        g.assocAdd(src, atype, dst,
                            TaoUpdates.MAX_TIME, TaoUpdates.ATTR_FOR_NEW_EDGES);

                        g.restartTransaction();
                }
            }

            @Override
            public int query() {
                int i, src, atype, dst;
                switch (chooseQuery(rand)) {
                    case 0:
                        // assoc_range
                        i = rand.nextInt(assocRange_query);
                        return g.assocRange(
                            assocRangeNodes[i],
                            assocRangeAtypes[i],
                            assocRangeOffsets[i],
                            assocRangeLengths[i]).size();
                    case 1:
                        // obj_get
                        i = rand.nextInt(objGet_query);
                        return g.objGet(objGetIds[i]).size();
                    case 2:
                        // assoc_get
                        i = rand.nextInt(assocGet_query);
                        return g.assocGet(
                            assocGetNodes[i],
                            assocGetAtypes[i],
//                                assocGetDstIdSets[i],
                            new HashSet(Arrays.asList(assocGetDstIdSets[i])),
                            assocGetTimeLows[i],
                            assocGetTimeHighs[i]).size();
                    case 3:
                        // assoc_count
                        i = rand.nextInt(assocCount_query);
                        g.assocCount(
                            assocCountNodes[i],
                            assocCountAtypes[i]);
                        return 1;
                    case 4:
                        // assoc_time_range
                        i = rand.nextInt(assocTimeRange_query);
                        return g.assocTimeRange(
                            assocTimeRangeNodes[i],
                            assocTimeRangeAtypes[i],
                            assocTimeRangeTimeLows[i],
                            assocTimeRangeTimeHighs[i],
                            assocTimeRangeLimits[i]).size();
                    case 5:
                        // assoc_add
                        src = rand.nextInt(TaoUpdates.NUM_NODES);
                        atype = rand.nextInt(TaoUpdates.NUM_ATYPES);
                        dst = rand.nextInt(TaoUpdates.NUM_NODES);

                        g.assocAdd(src, atype, dst,
                            TaoUpdates.MAX_TIME, TaoUpdates.ATTR_FOR_NEW_EDGES);

                        g.restartTransaction();
                }
                return 0;
            }
        };
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
