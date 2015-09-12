package edu.berkeley.cs.benchmark;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

public class Node extends Benchmark {
    public static final String WARMUP_FILE = "node_warmup_100000.txt";
    public static final String QUERY_FILE = "node_query_100000.txt";

    @Override
    public void readQueries() {
        getNodeQueries(WARMUP_FILE, warmupNodeAttrIds1, warmupNodeAttrIds2, warmupNodeAttrs1, warmupNodeAttrs2);
        getNodeQueries(QUERY_FILE, nodeAttrIds1, nodeAttrIds2, nodeAttrs1, nodeAttrs2);
    }

    @Override
    public int warmupQuery(int i) {
        return g.getNodes(modGet(warmupNodeAttrIds1, i), modGet(warmupNodeAttrs1, i)).size();
    }

    @Override
    public int query(int i) {
        return g.getNodes(modGet(nodeAttrIds1, i), modGet(nodeAttrs1, i)).size();
    }

    static void getNodeQueries(
            String file, List<Integer> indices1, List<Integer> indices2,
            List<String> queries1, List<String> queries2) {

        try {
            BufferedReader br = new BufferedReader(new FileReader(queryPath + "/" + file));
            String line = br.readLine();
            while (line != null) {
                String[] tokens = line.split("\\x02");
                indices1.add(Integer.parseInt(tokens[0]));
                queries1.add(tokens[1]);
                indices2.add(Integer.parseInt(tokens[2]));
                queries2.add(tokens[3]);
                line = br.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
