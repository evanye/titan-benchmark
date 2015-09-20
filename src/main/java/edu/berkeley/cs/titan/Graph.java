package edu.berkeley.cs.titan;

import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.util.TitanId;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import java.net.URL;
import java.util.*;

public class Graph {
    private TitanTransaction txn;

    // TitanGraph is static so is shared among multiple instances of Graph
    private static TitanGraph g = null;
    private static int numAtypes;
    private static int numProperties;
    private static int offset;
    private static EdgeLabel[] intToAtype;

    public Graph() {
        if (g == null) {
            URL props = getClass().getResource("/titan-cassandra.properties");
            URL configUrl = getClass().getResource("/benchmark.properties");

            Configuration titanConfiguration = null, config = null;
            try {
                config = new PropertiesConfiguration(configUrl);
                titanConfiguration = new PropertiesConfiguration(props);
                titanConfiguration.setProperty("storage.cassandra.keyspace", config.getString("name"));
            } catch (ConfigurationException e) {
                e.printStackTrace();
            }
            g = TitanFactory.open(titanConfiguration);

            numAtypes = config.getInt("atype.total");
            numProperties = config.getInt("property.total");
            offset = config.getBoolean("zero_indexed") ? 1 : 0;
            intToAtype = new EdgeLabel[numAtypes];
            for (int i = 0; i < numAtypes; i++) {
                intToAtype[i] = g.getEdgeLabel(String.valueOf(i));
            }
        }

        txn = g.buildTransaction().start();
    }

    public void restartTransaction() {
        txn.commit();
        txn = g.buildTransaction().start();
    }

    public List<Long> getNeighbors(long id) {
        List<Long> neighbors = new ArrayList<>();
        TitanVertex node = getNode(id);
        for (TitanEdge edge: node.getTitanEdges(Direction.OUT)) {
            neighbors.add(getId(edge.getOtherVertex(node)));
        }
        return neighbors;
    }

    public Set<Long> getNodes(int propIdx, String search) {
        Set<Long> nodeIds = new HashSet<>();
        for (Vertex v: txn.getVertices("attr" + propIdx, search)) {
            nodeIds.add(getId(v));
        }
        return nodeIds;
    }

    public Set<Long> getNodes(int propIdx1, String search1, int propIdx2, String search2) {
        Set<Long> ids1 = getNodes(propIdx1, search1);
        Set<Long> ids2 = getNodes(propIdx2, search2);
        ids1.retainAll(ids2);
        return ids1;
    }

    public List<Long> getNeighborNode(long id, int propIdx, String search) {
        List<Long> result = new ArrayList<>();
        TitanVertex node = getNode(id);
        for (TitanEdge edge: node.getTitanEdges(Direction.OUT)) {
            TitanVertex neighbor = edge.getOtherVertex(node);
            if (search.equals(neighbor.getProperty("attr" + propIdx))) {
                result.add(getId(neighbor));
            }
        }
        return result;
    }

    public List<Long> getNeighborAtype(long id, int atypeIdx) {
        List<TimestampedItem<Long>> neighbors = new ArrayList<>();
        TitanVertex node = getNode(id);
        for (TitanEdge edge: node.getTitanEdges(Direction.OUT, intToAtype[atypeIdx])) {
            TitanVertex neighbor = edge.getOtherVertex(node);
            neighbors.add(new TimestampedItem<>(
                    (long) edge.getProperty("timestamp"), getId(neighbor)
            ));
        }
        Collections.sort(neighbors);

        List<Long> result = new ArrayList<>();
        for (TimestampedItem<Long> neighbor: neighbors) {
            result.add(neighbor.item);
        }
        return result;
    }

    public List<String> getEdgeAttrs(long id, int atypeIdx) {
        List<TimestampedItem<String>> edges = new ArrayList<>();
        TitanVertex node = getNode(id);
        for (TitanEdge e: node.getTitanEdges(Direction.OUT, intToAtype[atypeIdx])) {
            edges.add(new TimestampedItem<>(
                    getId(e.getOtherVertex(node)), (String) e.getProperty("property")
            ));
        }
        Collections.sort(edges);
        List<String> results = new ArrayList<>(edges.size());
        for (TimestampedItem<String> edge: edges) {
            results.add(edge.item);
        }
        return results;
    }

    /**
     * TAO Queries
     */

    public List<String> objGet(long id) {
        TitanVertex node = getNode(id);
        List<String> results = new ArrayList<>();
        for (int propIdx = 0; propIdx < numProperties; propIdx++) {
            results.add((String) node.getProperty("attr" + propIdx));
        }
        return results;
    }

    public List<Assoc> assocRange(long id, int atype, int offset, int length) {
        TitanVertex node = getNode(id);
        List<Assoc> assocs = new ArrayList<>();

        for (TitanEdge edge: node.getTitanEdges(Direction.OUT, intToAtype[atype])) {
            assocs.add(new Assoc(edge));
        }

        if (offset < 0 || offset >= assocs.size()) return Collections.emptyList();
        Collections.sort(assocs);
        return assocs.subList(offset, Math.min(assocs.size(), offset + length));
    }

    // TODO: use timestamp sort key
    public List<Assoc> assocGet(long id, int atype, Set<Long> dstIdSet, long low, long high) {
        TitanVertex node = getNode(id);
        List<Assoc> assocs = new ArrayList<>();
        Assoc assoc;
        for (TitanEdge edge: node.getTitanEdges(Direction.OUT, intToAtype[atype])) {
            if (dstIdSet.contains(TitanId.fromVertexID(edge.getOtherVertex(node)))) {
                assoc = new Assoc(edge);
                if (assoc.timestamp >= low && assoc.timestamp <= high) {
                    assocs.add(assoc);
                }
            }
        }
        Collections.sort(assocs);
        return assocs;
    }

    public long assocCount(long id, int atype) {
        TitanVertex node = getNode(id);
        long count = 0;
        for (TitanEdge edge: node.getTitanEdges(Direction.OUT, intToAtype[atype])) {
            ++count;
        }
        return count;
    }

    public List<Assoc> assocTimeRange(long id, int atype, long low, long high, int limit) {
        TitanVertex node = getNode(id);
        List<Assoc> assocs = new ArrayList<>();

        Assoc assoc;
        for (TitanEdge edge: node.getTitanEdges(Direction.OUT, intToAtype[atype])) {
            assoc = new Assoc(edge);
            if (assoc.timestamp >= low && assoc.timestamp <= high) {
                assocs.add(assoc);
            }
        }
        Collections.sort(assocs);
        return assocs.subList(0, Math.min(limit, assocs.size()));
    }

    public void warmup() {
        restartTransaction();
        long c = 0L;
        for (Vertex v: txn.getVertices()) {
            v.getId();
            for (String key: v.getPropertyKeys()) {
                Object prop = v.getProperty(key);
            }
            for (Edge e: v.getEdges(Direction.OUT)) {
                e.getVertex(Direction.IN);
            }
            if (++c % 10000 == 0)
                System.out.println("processed " + c + " nodes");
        }

        restartTransaction();
        c = 0L;
        for (Edge e: txn.getEdges()) {
            e.getId();
            e.getLabel();
            e.getVertex(Direction.IN); e.getVertex(Direction.OUT);
            for (String key: e.getPropertyKeys()) {
                Object prop = e.getProperty(key);
            }
            if (++c % 10000 == 0)
                System.out.println("processed " + c + " edges");
        }
        restartTransaction();
    }

    protected static long getId(TitanVertex v) {
        return TitanId.fromVertexID(v) - offset;
    }

    protected static long getId(Vertex v) {
        long titanId = (long) v.getId();
        return TitanId.fromVertexId(titanId) - offset;
    }

    protected TitanVertex getNode(long id) {
        return txn.getVertex(TitanId.toVertexId(id + offset));
    }

    public static void shutdown() {
        g.shutdown();
    }

    class TimestampedItem<T> implements Comparable<TimestampedItem<T>> {
        long timestamp;
        T item;
        public TimestampedItem(long timestamp, T item) {
            this.timestamp = timestamp;
            this.item = item;
        }
        @Override
        // Larger timestamp comes first.
        public int compareTo(TimestampedItem<T> o) {
            return Long.compare(o.timestamp, this.timestamp);
        }
    }
}

