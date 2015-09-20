package edu.berkeley.cs.benchmark;

import edu.berkeley.cs.titan.Graph;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Set;

public class Node extends Benchmark<Set<Long>> {
    public static final String WARMUP_FILE = "node_warmup_100000.txt";
    public static final String QUERY_FILE = "node_query_100000.txt";

    @Override
    public void readQueries() {
        getNodeQueries(WARMUP_FILE, warmupNodeAttrIds1, warmupNodeAttrIds2, warmupNodeAttrs1, warmupNodeAttrs2);
        getNodeQueries(QUERY_FILE, nodeAttrIds1, nodeAttrIds2, nodeAttrs1, nodeAttrs2);
    }

    @Override
    public Set<Long> warmupQuery(Graph g, int i) {
        return g.getNodes(modGet(warmupNodeAttrIds1, i), modGet(warmupNodeAttrs1, i));
    }

    @Override
    public Set<Long> query(Graph g, int i) {
        return g.getNodes(modGet(nodeAttrIds1, i), modGet(nodeAttrs1, i));
    }

    @Override
    public RunThroughput getThroughputJob(int clientId) {
        return new RunThroughput(clientId) {
            @Override
            public void warmupQuery() {
                Node.this.warmupQuery(g, rand.nextInt(warmupNodeAttrIds1.size()));
            }

            @Override
            public int query() {
                int idx = rand.nextInt(nodeAttrIds1.size());
                return Node.this.query(g, idx).size();
            }
        };
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
