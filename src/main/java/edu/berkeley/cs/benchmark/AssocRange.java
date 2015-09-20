package edu.berkeley.cs.benchmark;

import edu.berkeley.cs.titan.Assoc;
import edu.berkeley.cs.titan.Graph;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

public class AssocRange extends Benchmark<List<Assoc>> {
    public static final String WARMUP_FILE = "assocRange_warmup.txt";
    public static final String QUERY_FILE = "assocRange_query.txt";

    @Override
    public void readQueries() {
        readAssocRangeQueries(WARMUP_FILE,
                warmupAssocRangeNodes, warmupAssocRangeAtypes,
                warmupAssocRangeOffsets, warmupAssocRangeLengths);
        readAssocRangeQueries(QUERY_FILE,
                assocRangeNodes, assocRangeAtypes,
                assocRangeOffsets, assocRangeLengths);
    }

    @Override
    public List<Assoc> warmupQuery(Graph g, int i) {
        return g.assocRange(
                modGet(warmupAssocRangeNodes, i),
                modGet(warmupAssocRangeAtypes, i),
                modGet(warmupAssocRangeOffsets, i),
                modGet(warmupAssocRangeLengths, i));
    }

    @Override
    public List<Assoc> query(Graph g, int i) {
        return g.assocRange(
                modGet(assocRangeNodes, i),
                modGet(assocRangeAtypes, i),
                modGet(assocRangeOffsets, i),
                modGet(assocRangeLengths, i));
    }

    @Override
    public RunThroughput getThroughputJob(int clientId) {
        return new RunThroughput(clientId) {
            @Override
            public void warmupQuery() {
                int idx = rand.nextInt(warmupAssocRangeNodes.size());
                AssocRange.this.warmupQuery(g, idx);
            }

            @Override
            public int query() {
                int idx = rand.nextInt(assocRangeNodes.size());
                return AssocRange.this.query(g, idx).size();
            }
        };
    }

    static void readAssocRangeQueries(
            String file, List<Long> nodes, List<Integer> atypes,
            List<Integer> offsets, List<Integer> lengths) {

        try {
            BufferedReader br = new BufferedReader(new FileReader(queryPath + "/" + file));
            String line = br.readLine();
            while (line != null) {
                int idx = line.indexOf(',');
                nodes.add(Long.parseLong(line.substring(0, idx)));

                int idx2 = line.indexOf(',', idx + 1);
                atypes.add(Integer.parseInt(line.substring(idx + 1, idx2)));

                int idx3 = line.indexOf(',', idx2 + 1);
                offsets.add(Integer.parseInt(line.substring(idx2 + 1, idx3)));

                lengths.add(Integer.parseInt(line.substring(idx3 + 1)));
                line = br.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
