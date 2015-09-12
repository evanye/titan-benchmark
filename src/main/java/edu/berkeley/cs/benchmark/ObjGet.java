package edu.berkeley.cs.benchmark;

public class ObjGet extends Benchmark {
    public static final String WARMUP_FILE = "objGet_warmup.txt";
    public static final String QUERY_FILE = "objGet_query.txt";

    @Override
    public void readQueries() {
        getLong(WARMUP_FILE, warmupObjGetIds);
        getLong(QUERY_FILE, objGetIds);
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
