package edu.berkeley.cs.benchmark;

import edu.berkeley.cs.titan.Graph;

import java.util.Set;

public class NodeNode extends Node {

    @Override
    public Set<Long> warmupQuery(Graph g, int i) {
        return g.getNodes(warmupNodeAttrIds1[i], warmupNodeAttrs1[i],
                warmupNodeAttrIds2[i], warmupNodeAttrs2[i]);
    }

    @Override
    public Set<Long> query(Graph g, int i) {
        return g.getNodes(nodeAttrIds1[i], nodeAttrs1[i],
                nodeAttrIds2[i], nodeAttrs2[i]);
    }

    @Override
    public RunThroughput getThroughputJob(int clientId) {
        return new RunThroughput(clientId) {
            @Override
            public void warmupQuery() {
                int idx = rand.nextInt(node_warmup);
                NodeNode.this.warmupQuery(g, rand.nextInt(idx));
            }

            @Override
            public int query() {
                int idx = rand.nextInt(node_query);
                return NodeNode.this.query(g, idx).size();
            }
        };
    }
}
