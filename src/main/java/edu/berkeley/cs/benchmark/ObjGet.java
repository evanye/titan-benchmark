package edu.berkeley.cs.benchmark;

import edu.berkeley.cs.titan.Graph;

import java.util.List;

public class ObjGet extends Benchmark<List<String>> {
    public static final String WARMUP_FILE = "objGet_warmup.txt";
    public static final String QUERY_FILE = "objGet_query.txt";

    @Override
    public void readQueries() {
        getLong(WARMUP_FILE, warmupObjGetIds);
        getLong(QUERY_FILE, objGetIds);
    }

    @Override
    public List<String> warmupQuery(Graph g, int i) {
        return g.objGet(modGet(warmupObjGetIds, i));
    }

    @Override
    public List<String> query(Graph g, int i) {
        return g.objGet(modGet(objGetIds, i));
    }
}
