package edu.berkeley.cs.benchmark;

import edu.berkeley.cs.titan.Graph;

public class NodeNode extends Node {

    @Override
    public int warmupQuery(Graph g, int i) {
        return g.getNodes(modGet(warmupNodeAttrIds1, i), modGet(warmupNodeAttrs1, i),
                modGet(warmupNodeAttrIds2, i), modGet(warmupNodeAttrs2, i)).size();
    }

    @Override
    public int query(Graph g, int i) {
        return g.getNodes(modGet(nodeAttrIds1, i), modGet(nodeAttrs1, i),
                modGet(nodeAttrIds2, i), modGet(nodeAttrs2, i)).size();
    }

    @Override
    public RunThroughput getThroughputJob(int clientId) {
        return new RunThroughput(clientId) {
            @Override
            public void warmupQuery() {
                NodeNode.this.warmupQuery(g, rand.nextInt(warmupNodeAttrIds1.size()));
            }

            @Override
            public int query() {
                int idx = rand.nextInt(nodeAttrIds1.size());
                return NodeNode.this.query(g, idx);
            }
        };
    }
}
