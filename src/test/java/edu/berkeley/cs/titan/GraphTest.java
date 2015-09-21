package edu.berkeley.cs.titan;

import edu.berkeley.cs.Load;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;

public class GraphTest {

    /**
     * Make sure cassandra is running, and
     * directory is pointing to /mnt/cassandra !
     */
    private Graph g;

    @BeforeClass
    public static void setUp() throws Exception {
        System.out.println("Setting up");
        Configuration config = new PropertiesConfiguration(
                GraphTest.class.getResource("/benchmark.properties")) {{
            setProperty("atype.total", 6);
            setProperty("data.node", GraphTest.class.getResource("/node").getPath());
            setProperty("data.edge", GraphTest.class.getResource("/edge").getPath());
        }};
        Configuration titanConfiguration = new PropertiesConfiguration(
                GraphTest.class.getResource("/titan-cassandra.properties"));

        Load.load(config, titanConfiguration);
    }

    @Before
    public void initalizeGraph() throws InterruptedException {
        g = new Graph();
    }

    private void assertListEquals(List<?> l1, Object l2) {
        List<?> l2list = null;
        if (l2 instanceof List<?>)
            l2list = (List<?>) l2;
        else if (l2 instanceof Set<?>)
            l2list = new LinkedList<>((Set<?>) l2);


        assertEquals("lists have different sizes!", l1.size(), l2list.size());
        for (int i = 0; i < l1.size(); i++) {
            assertEquals(String.valueOf(l1.get(i)), String.valueOf(l2list.get(i)));
        }
    }

    @Test
    public void testGetNeighbors() throws Exception {
        assertListEquals(Arrays.asList(6, 9, 2, 3, 4, 7, 8, 5), g.getNeighbors(1));
        assertListEquals(Arrays.asList(5, 6), g.getNeighbors(2));
        assertListEquals(Arrays.asList(), g.getNeighbors(3));
        assertListEquals(Arrays.asList(5, 7, 2, 9, 6, 8), g.getNeighbors(4));
        assertListEquals(Arrays.asList(2, 6, 8), g.getNeighbors(5));
        assertListEquals(Arrays.asList(2, 4, 9), g.getNeighbors(6));
        assertListEquals(Arrays.asList(6, 5, 8), g.getNeighbors(7));
    }

    @Test
    public void testGetNodes() throws Exception {
        assertListEquals(Arrays.asList(1, 10), g.getNodes(7, "VER IN PERSON|FO"));
        assertListEquals(Arrays.asList(5), g.getNodes(1, "|COLLECT COD|TRU"));
    }

    @Test
    public void testGetNodes1() throws Exception {
        assertListEquals(Arrays.asList(10), g.getNodes(7, "VER IN PERSON|FO", 0, "ounts doze again"));
        assertListEquals(Arrays.asList(), g.getNodes(7, "VER IN PERSON|FO", 1, "ounts doze again"));
        assertListEquals(Arrays.asList(8), g.getNodes(39, "-04-17|1993-05-1", 1, "lithely |677|348"));
    }

    @Test
    public void testGetNeighborNode() throws Exception {
        assertListEquals(Arrays.asList(2, 3), g.getNeighborNode(1, 39, "quests. special,"));
        assertListEquals(Arrays.asList(8), g.getNeighborNode(5, 9, "carefully final "));
        assertListEquals(Arrays.asList(), g.getNeighborNode(5, 4, "carefully final "));
    }

    @Test
    public void testGetNeighborAtype() throws Exception {
        assertListEquals(Arrays.asList(9, 6), g.getNeighborAtype(1, 0));
        assertListEquals(Arrays.asList(4, 3, 2), g.getNeighborAtype(1, 1));
        assertListEquals(Arrays.asList(), g.getNeighborAtype(5, 1));
    }

    @Test
    public void testObjGet() throws Exception {
        List<String> properties = g.objGet(9);
        assertEquals("NE|AIR|refully u", properties.get(0));
        assertEquals("ns sleep careful", properties.get(16));
        assertEquals("AKE BACK RETURN|", properties.get(39));
    }

    @Test
    public void testEdgeGet() throws Exception {
        assertListEquals(Arrays.asList(
                "64|12892632|892633|1|21|34103.79|0.05|0.02|R|F|1994-09-30|1994-09-18|1994-10-26|DELIVER IN PERSON|REG AIR|ch slyly final, thin p",
                "7|22784072|1409118|5|38|43887.72|0.08|0.01|N|O|1996-02-11|1996-02-24|1996-02-18|DELIVER IN PERSON|TRUCK|ns haggle carefully iron"),
                g.getEdgeAttrs(1, 0));
        assertListEquals(Arrays.asList(
                "1018497|27133722|508741|4|41|71929.17|0.05|0.03|A|F|1995-03-01|1995-01-22|1995-03-16|DELIVER IN PERSON|TRUCK|y. quickly ironic  ",
                "1018469|20276209|401249|1|15|17762.85|0.05|0.04|N|O|1997-07-11|1997-08-14|1997-08-04|DELIVER IN PERSON|TRUCK|ide of the bravely "),
                g.getEdgeAttrs(5, 5));
    }

    private List<Long> assocToId(List<Assoc> l) {
        List<Long> result = new LinkedList<>();
        for (Assoc a: l) {
            result.add(a.dstId);
        }
        return result;
    }
    @Test
    public void testAssocRange() throws Exception {
        assertListEquals(Arrays.asList(4, 3, 2),assocToId(g.assocRange(1, 1, 0, 1000)));
        assertListEquals(Arrays.asList(3, 2), assocToId(g.assocRange(1, 1, 1, 2)));
        assertListEquals(Arrays.asList(), assocToId(g.assocRange(1, 1, 10, 2)));
        assertListEquals(Arrays.asList(7, 5), assocToId(g.assocRange(4, 0, 0, 2)));
    }

    private static Set<Long> newHashSet(long... longs) {
        HashSet<Long> set = new HashSet<>();
        for (Long l : longs) {
            set.add(l);
        }
        return set;
    }

    @Test
    public void testAssocGet() throws Exception {
        assertListEquals(Arrays.asList(4, 3, 2),
                assocToId(g.assocGet(1, 1, newHashSet(1, 2, 3, 4, 5), 0, 1434934366842L)));
        assertListEquals(Arrays.asList(4, 2),
                assocToId(g.assocGet(1, 1, newHashSet(1, 2, 4, 5), 0, 1434934366842L)));
        assertListEquals(Arrays.asList(4, 3),
                assocToId(g.assocGet(1, 1, newHashSet(1, 2, 3, 4, 5), 1433950511513L, 1434934366842L)));
        assertListEquals(Arrays.asList(3),
                assocToId(g.assocGet(1, 1, newHashSet(1, 2, 3, 4, 5), 1433950511513L, 1434704348505L)));
    }

    @Test
    public void testAssocCount() throws Exception {
        assertEquals(2, g.assocCount(1, 0));
        assertEquals(3, g.assocCount(1, 1));
        assertEquals(0, g.assocCount(9, 0));
        assertEquals(0, g.assocCount(8, 0));
    }

    @Test
    public void testAssocTimeRange() throws Exception {
        assertListEquals(Arrays.asList(4, 3, 2),
                assocToId(g.assocTimeRange(1, 1, 0, 1434934366842L, 100)));
        assertListEquals(Arrays.asList(4, 3),
                assocToId(g.assocTimeRange(1, 1, 0, 1434934366842L, 2)));
        assertListEquals(Arrays.asList(3, 2),
                assocToId(g.assocTimeRange(1, 1, 0, 1434704348505L, 2)));
        assertListEquals(Arrays.asList(2),
                assocToId(g.assocTimeRange(6, 0, 1432977104991L, 1433364960621L, 2)));
    }
}