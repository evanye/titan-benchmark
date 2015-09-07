package edu.berkeley.cs;

import com.thinkaurelius.titan.core.TitanEdge;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.core.util.TitanId;
import com.tinkerpop.blueprints.Direction;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import java.net.URL;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class Graph {
    final TitanGraph g;
    public Graph() {
        URL props = Load.class.getResource("/titan-cassandra.properties");
        Configuration titanConfiguration = null;
        try {
            titanConfiguration = new PropertiesConfiguration(props);
        } catch (ConfigurationException e) {
            e.printStackTrace();
        }

        g = TitanFactory.open(titanConfiguration);
    }

    public List<Long> getNeighbors(long id) {
        List<Long> neighbors = new LinkedList<>();
        TitanVertex node = g.getVertex(TitanId.fromVertexId(id));
        for (TitanEdge edge: node.getTitanEdges(Direction.OUT)) {
            neighbors.add(TitanId.fromVertexID(edge.getOtherVertex(node)));
        }
        return neighbors;
    }

    public Set<Long> getNodes(int propIdx, String search) {
        g.
    }

    public void shutdown() {
        g.shutdown();
    }
}
