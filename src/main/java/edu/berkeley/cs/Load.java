package edu.berkeley.cs;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.thinkaurelius.titan.core.util.TitanId;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.wrappers.batch.BatchGraph;
import com.tinkerpop.blueprints.util.wrappers.batch.VertexIDType;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import java.io.*;
import java.net.URL;
import java.util.List;

public class Load {

    public static void main(String[] args) throws ConfigurationException, IOException {
        final Configuration config = new PropertiesConfiguration(Load.class.getResource("/benchmark.properties"));

        URL props = Load.class.getResource("/titan-cassandra.properties");
        Configuration titanConfiguration = new PropertiesConfiguration(props) {{
            setProperty("storage.batch-loading", true);
            setProperty("schema.default", "none");
            setProperty("graph.set-vertex-id", true);
        }};

        TitanGraph g = TitanFactory.open(titanConfiguration);
        createSchemaIfNotExists(g, config);
        loadGraph(g, config);
        System.exit(0);
    }

    private static void createSchemaIfNotExists(TitanGraph g, Configuration config) {
        TitanManagement mgmt = g.getManagementSystem();
        if (mgmt.containsEdgeLabel("0"))
            return;

        int NUM_ATTR = config.getInt("property.total");
        int NUM_ATYPES = config.getInt("atype.total");

        PropertyKey[] nodeProperties = new PropertyKey[NUM_ATTR];
        for (int i = 0; i < NUM_ATTR; i++) {
            nodeProperties[i] = mgmt.makePropertyKey("attr" + i).dataType(String.class).make();
            mgmt.buildIndex("byAttr" + i, Vertex.class).addKey(nodeProperties[i]).buildCompositeIndex();
        }

        PropertyKey timestamp = mgmt.makePropertyKey("timestamp").dataType(Long.class).make();
        PropertyKey edgeProperty = mgmt.makePropertyKey("property").dataType(String.class).make();
        for (int i = 0; i < NUM_ATYPES; i++) {
            EdgeLabel label = mgmt.makeEdgeLabel(""+ i).signature(timestamp, edgeProperty).unidirected().make();
            mgmt.buildEdgeIndex(label, "byEdge"+i, Direction.OUT, Order.DESC, timestamp);
        }

        mgmt.commit();
    }

    private static void loadGraph(TitanGraph g, Configuration conf) throws IOException {
        BatchGraph bg = new BatchGraph(g, VertexIDType.NUMBER, 10000);

        int propertySize = conf.getInt("property.size");
        long c = 1L;
        try (BufferedReader br = new BufferedReader(new FileReader(conf.getString("data.node")))) {
            for (String line; (line = br.readLine()) != null; ) {
                Vertex node = bg.addVertex(TitanId.toVertexId(c));
                int i = 0;
                for (String attr: Splitter.fixedLength(propertySize + 1).split(line)) {
                    attr = attr.substring(1); // trim first delimiter character
                    node.setProperty("attr" + i, attr);
                    i++;
                }
                if (++c%100000L == 0L) {
                    System.out.println("Processed " + c + " nodes");
                }
            }
        }

        c = 1L;
        try (BufferedReader br = new BufferedReader(new FileReader(conf.getString("data.edge")))) {
            for (String line; (line = br.readLine()) != null; ) {
                List<String> tokens = Lists.newArrayList(Splitter.on(' ').limit(5).trimResults().split(line));
                Long id1 = Long.parseLong(tokens.get(0));
                Long id2 = Long.parseLong(tokens.get(1));
                String atype = tokens.get(2);
                Long timestamp = Long.parseLong(tokens.get(3));
                String property = tokens.get(4);

                Vertex v1 = bg.getVertex(TitanId.toVertexId(id1));
                Vertex v2 = bg.getVertex(TitanId.toVertexId(id2));
                Edge edge = bg.addEdge(null, v1, v2, atype);
                edge.setProperty("timestamp", timestamp);
                edge.setProperty("property", property);
                if (++c%100000L == 0L) {
                    System.out.println("Processed " + c + " edges");
                }
            }
        }

        bg.commit();
    }
}
