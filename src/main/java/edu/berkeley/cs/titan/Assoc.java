package edu.berkeley.cs.titan;

import com.thinkaurelius.titan.core.TitanEdge;
import com.tinkerpop.blueprints.Direction;

public class Assoc implements Comparable<Assoc> {
    public long srcId, dstId, atype;
    public long timestamp;
    public String prop; // For now assumes one edge attribute

    public Assoc(TitanEdge edge) {
        this.srcId = Graph.getId(edge.getVertex(Direction.OUT));
        this.dstId = Graph.getId(edge.getVertex(Direction.IN));
        this.atype = Integer.valueOf(String.valueOf(edge.getEdgeLabel()));
        this.timestamp = edge.getProperty("timestamp");
        this.prop = edge.getProperty("property");
    }

    public String toString() {
        return String.format(
                "[src=%d,dst=%d,atype=%d,time=%d,prop='%s']",
                srcId, dstId, atype, timestamp, prop);
    }

    @Override
    // When we sort assocs, we want them in most recent to least recent
    public int compareTo(Assoc o) {
        return Long.compare(o.timestamp, this.timestamp);
    }
}
