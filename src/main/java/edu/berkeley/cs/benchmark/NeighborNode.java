package edu.berkeley.cs.benchmark;

import edu.berkeley.cs.titan.Graph;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

public class NeighborNode extends Benchmark<List<Long>> {
    public static final String WARMUP_FILE = "neighbor_node_warmup_100000.txt";
    public static final String QUERY_FILE = "neighbor_node_query_100000.txt";

    @Override
    public void readQueries() {
        getNeighborNodeQueries(WARMUP_FILE, warmupNeighborNodeIds, warmupNeighborNodeAttrIds, warmupNeighborNodeAttrs);
        getNeighborNodeQueries(QUERY_FILE, neighborNodeIds, neighborNodeAttrIds, neighborNodeAttrs);
    }

    @Override
    public List<Long> warmupQuery(Graph g, int i) {
        return g.getNeighborNode(warmupNeighborNodeIds[i],
                warmupNeighborNodeAttrIds[i], warmupNeighborNodeAttrs[i]);
    }

    @Override
    public List<Long> query(Graph g, int i) {
        return g.getNeighborNode(neighborNodeIds[i],
                neighborNodeAttrIds[i], neighborNodeAttrs[i]);
    }

    @Override
    public RunThroughput getThroughputJob(int clientId) {
        return new RunThroughput(clientId) {
            @Override
            public void warmupQuery() {
                int idx = rand.nextInt(neighborNode_warmup);
                NeighborNode.this.warmupQuery(g, idx);
            }

            @Override
            public int query() {
                int idx = rand.nextInt(neighborNode_query);
                return NeighborNode.this.query(g, idx).size();
            }
        };
    }

    static void getNeighborNodeQueries(
            String file, long[] indices,
            int[] attributes, String[] queries) {

        try {
            BufferedReader br = new BufferedReader(new FileReader(queryPath + "/" + file));
            for (int i = 0; i < indices.length; i++) {
                String line = br.readLine();
                int idx = line.indexOf(',');
                indices[i] = Long.parseLong(line.substring(0, idx));
                int idx2 = line.indexOf(',', idx + 1);
                attributes[i] = Integer.parseInt(line.substring(idx + 1, idx2));
                queries[i] = line.substring(idx2 + 1);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
