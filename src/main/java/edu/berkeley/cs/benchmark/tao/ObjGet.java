package edu.berkeley.cs.benchmark.tao;

import edu.berkeley.cs.benchmark.Benchmark;

public class ObjGet extends BenchTao {
    @Override
    public void readQueries() {
        Benchmark.getNeighborQueries(queryPath + "/objGet_warmup.txt", warmupObjGetIds);
        Benchmark.getNeighborQueries(queryPath + "/objGet_query.txt", objGetIds);
    }

    @Override
    public int warmupQuery(int i) {
        return g.objGet(modGet(warmupObjGetIds, i)).size();
    }

    @Override
    public int query(int i) {
        return g.objGet(modGet(objGetIds, i)).size();
    }
}
