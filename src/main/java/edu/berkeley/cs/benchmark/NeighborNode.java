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
        return g.getNeighborNode(modGet(warmupNeighborNodeIds, i),
                modGet(warmupNeighborNodeAttrIds, i), modGet(warmupNeighborNodeAttrs, i));
    }

    @Override
    public List<Long> query(Graph g, int i) {
        return g.getNeighborNode(modGet(neighborNodeIds, i),
                modGet(neighborNodeAttrIds, i), modGet(neighborNodeAttrs, i));
    }

    @Override
    public RunThroughput getThroughputJob(int clientId) {
        return new RunThroughput(clientId) {
            @Override
            public void warmupQuery() {
                NeighborNode.this.warmupQuery(g, rand.nextInt(warmupNeighborNodeIds.size()));
            }

            @Override
            public int query() {
                int idx = rand.nextInt(neighborNodeIds.size());
                return NeighborNode.this.query(g, idx).size();
            }
        };
    }

    static void getNeighborNodeQueries(
            String file, List<Long> indices,
            List<Integer> attributes, List<String> queries) {

        try {
            BufferedReader br = new BufferedReader(new FileReader(queryPath + "/" + file));
            String line = br.readLine();
            while (line != null) {
                int idx = line.indexOf(',');
                indices.add(Long.parseLong(line.substring(0, idx)));
                int idx2 = line.indexOf(',', idx + 1);
                attributes.add(Integer.parseInt(line.substring(idx + 1, idx2)));
                queries.add(line.substring(idx2 + 1));
                line = br.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
