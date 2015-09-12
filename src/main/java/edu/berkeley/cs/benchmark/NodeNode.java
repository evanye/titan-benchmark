package edu.berkeley.cs.benchmark;

public class NodeNode extends Node {

    @Override
    public int warmupQuery(int i) {
        return g.getNodes(modGet(warmupNodeAttrIds1, i), modGet(warmupNodeAttrs1, i),
                modGet(warmupNodeAttrIds2, i), modGet(warmupNodeAttrs2, i)).size();
    }

    @Override
    public int query(int i) {
        return g.getNodes(modGet(nodeAttrIds1, i), modGet(nodeAttrs1, i),
                modGet(nodeAttrIds2, i), modGet(nodeAttrs2, i)).size();
    }

}
