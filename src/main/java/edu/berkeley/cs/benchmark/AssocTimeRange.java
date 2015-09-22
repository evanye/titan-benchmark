package edu.berkeley.cs.benchmark;

import edu.berkeley.cs.titan.Assoc;
import edu.berkeley.cs.titan.Graph;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

public class AssocTimeRange extends Benchmark<List<Assoc>> {
    public static final String WARMUP_FILE = "assocTimeRange_warmup.txt";
    public static final String QUERY_FILE = "assocTimeRange_query.txt";

    @Override
    public void readQueries() {
        readAssocTimeRangeQueries(WARMUP_FILE,
                warmupAssocTimeRangeNodes,
                warmupAssocTimeRangeAtypes, warmupAssocTimeRangeTimeLows,
                warmupAssocTimeRangeTimeHighs, warmupAssocTimeRangeLimits);

        readAssocTimeRangeQueries(QUERY_FILE,
                assocTimeRangeNodes, assocTimeRangeAtypes,
                assocTimeRangeTimeLows, assocTimeRangeTimeHighs,
                assocTimeRangeLimits);
    }

    @Override
    public List<Assoc> warmupQuery(Graph g, int i) {
        return g.assocTimeRange(
                warmupAssocTimeRangeNodes[i],
                warmupAssocTimeRangeAtypes[i],
                warmupAssocTimeRangeTimeLows[i],
                warmupAssocTimeRangeTimeHighs[i],
                warmupAssocTimeRangeLimits[i]);
    }

    @Override
    public List<Assoc> query(Graph g, int i) {
        return g.assocTimeRange(
                assocTimeRangeNodes[i],
                assocTimeRangeAtypes[i],
                assocTimeRangeTimeLows[i],
                assocTimeRangeTimeHighs[i],
                assocTimeRangeLimits[i]);
    }

    static void readAssocTimeRangeQueries(
            String file, long[] nodes, int[] atypes,
            long[] tLows, long[] tHighs, int[] limits) {

        try {
            BufferedReader br = new BufferedReader(new FileReader(queryPath + "/" + file));
            for (int i = 0; i < nodes.length; i++) {
                String line = br.readLine();
                int idx = line.indexOf(',');
                nodes[i] = (Long.parseLong(line.substring(0, idx)));

                int idx2 = line.indexOf(',', idx + 1);
                atypes[i] = (Integer.parseInt(line.substring(idx + 1, idx2)));

                int idx3 = line.indexOf(',', idx2 + 1);
                tLows[i] = (Long.parseLong(line.substring(idx2 + 1, idx3)));

                int idx4 = line.indexOf(',', idx3 + 1);
                tHighs[i] = (Long.parseLong(line.substring(idx3 + 1, idx4)));

                limits[i] = (Integer.parseInt(line.substring(idx4 + 1)));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
