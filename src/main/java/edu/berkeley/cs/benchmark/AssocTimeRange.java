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
                modGet(warmupAssocTimeRangeNodes, i),
                modGet(warmupAssocTimeRangeAtypes, i),
                modGet(warmupAssocTimeRangeTimeLows, i),
                modGet(warmupAssocTimeRangeTimeHighs, i),
                modGet(warmupAssocTimeRangeLimits, i));
    }

    @Override
    public List<Assoc> query(Graph g, int i) {
        return g.assocTimeRange(
                modGet(assocTimeRangeNodes, i),
                modGet(assocTimeRangeAtypes, i),
                modGet(assocTimeRangeTimeLows, i),
                modGet(assocTimeRangeTimeHighs, i),
                modGet(assocTimeRangeLimits, i));
    }

    static void readAssocTimeRangeQueries(
            String file, List<Long> nodes, List<Integer> atypes,
            List<Long> tLows, List<Long> tHighs, List<Integer> limits) {

        try {
            BufferedReader br = new BufferedReader(new FileReader(queryPath + "/" + file));
            String line = br.readLine();
            while (line != null) {
                int idx = line.indexOf(',');
                nodes.add(Long.parseLong(line.substring(0, idx)));

                int idx2 = line.indexOf(',', idx + 1);
                atypes.add(Integer.parseInt(line.substring(idx + 1, idx2)));

                int idx3 = line.indexOf(',', idx2 + 1);
                tLows.add(Long.parseLong(line.substring(idx2 + 1, idx3)));

                int idx4 = line.indexOf(',', idx3 + 1);
                tHighs.add(Long.parseLong(line.substring(idx3 + 1, idx4)));

                limits.add(Integer.parseInt(line.substring(idx4 + 1)));

                line = br.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
