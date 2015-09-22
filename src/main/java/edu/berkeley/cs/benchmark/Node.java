package edu.berkeley.cs.benchmark;

import edu.berkeley.cs.titan.Graph;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
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
        return g.getNodes(warmupNodeAttrIds1[i], warmupNodeAttrs1[i]);
    }

    @Override
    public Set<Long> query(Graph g, int i) {
        return g.getNodes(nodeAttrIds1[i], nodeAttrs1[i]);
    }

    @Override
    public RunThroughput getThroughputJob(int clientId) {
        return new RunThroughput(clientId) {
            @Override
            public void warmupQuery() {
                int idx = rand.nextInt(node_warmup);
                Node.this.warmupQuery(g, idx);
            }

            @Override
            public int query() {
                int idx = rand.nextInt(node_query);
                return Node.this.query(g, idx).size();
            }
        };
    }

    static void getNodeQueries(
            String file, int[] indices1, int[] indices2,
            String[] queries1, String[] queries2) {

        try {
            BufferedReader br = new BufferedReader(new FileReader(queryPath + "/" + file));
            for (int i = 0; i < indices1.length; i++) {
                String line = br.readLine();
                String[] tokens = line.split("\\x02");
                indices1[i] = Integer.parseInt(tokens[0]);
                queries1[i] = tokens[1];
                indices2[i] = Integer.parseInt(tokens[2]);
                queries2[i] = tokens[3];
                line = br.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
