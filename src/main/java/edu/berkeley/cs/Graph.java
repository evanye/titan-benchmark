package edu.berkeley.cs;

import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.util.TitanId;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import java.net.URL;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class Graph {
    final TitanGraph g;
    private TitanTransaction txn;

    public Graph() {
        URL props = getClass().getResource("/titan-cassandra.properties");
        Configuration titanConfiguration = null;
        try {
            titanConfiguration = new PropertiesConfiguration(props);
        } catch (ConfigurationException e) {
            e.printStackTrace();
        }

        g = TitanFactory.open(titanConfiguration);
        txn = g.buildTransaction().readOnly().start();
    }

    public void restartTransaction() {
        txn.commit();
        txn = g.buildTransaction().readOnly().start();
    }

    public List<Long> getNeighbors(long id) {
        List<Long> neighbors = new LinkedList<>();
        TitanVertex node = txn.getVertex(TitanId.fromVertexId(id));
        for (TitanEdge edge: node.getTitanEdges(Direction.OUT)) {
            neighbors.add(TitanId.fromVertexID(edge.getOtherVertex(node)));
        }
        return neighbors;
    }

    public Set<Long> getNodes(int propIdx, String search) {
        Set<Long> nodeIds = new HashSet<>();
        for (Vertex v: txn.getVertices("attr" + propIdx, search)) {
            long id = Long.parseLong("" + v.getId());
            nodeIds.add(TitanId.fromVertexId(id));
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
        List<Long> result = new LinkedList<>();
        TitanVertex node = txn.getVertex(TitanId.fromVertexId(id));
        for (TitanEdge edge: node.getTitanEdges(Direction.OUT)) {
            TitanVertex neighbor = edge.getOtherVertex(node);
            if (search.equals(neighbor.getProperty("attr" + propIdx))) {
                result.add(TitanId.fromVertexID(neighbor));
            }
        }
        return result;
    }

    public void warmup() {
        restartTransaction();
        for (Vertex v: txn.getVertices()) {
            v.getId();
            for (String key: v.getPropertyKeys()) {
                Object prop = v.getProperty(key);
            }
            for (Edge e: v.getEdges(Direction.OUT)) {
                e.getVertex(Direction.IN);
            }
        }

        for (Edge e: txn.getEdges()) {
            e.getId();
            e.getLabel();
            e.getVertex(Direction.IN); e.getVertex(Direction.OUT);
            for (String key: e.getPropertyKeys()) {
                Object prop = e.getProperty(key);
            }
        }
        restartTransaction();
    }

    public void shutdown() {
        g.shutdown();
    }
}
