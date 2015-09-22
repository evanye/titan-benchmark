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
                warmupAssocRangeNodes[i],
                warmupAssocRangeAtypes[i],
                warmupAssocRangeOffsets[i],
                warmupAssocRangeLengths[i]);
    }

    @Override
    public List<Assoc> query(Graph g, int i) {
        return g.assocRange(
                assocRangeNodes[i],
                assocRangeAtypes[i],
                assocRangeOffsets[i],
                assocRangeLengths[i]);
    }

    @Override
    public RunThroughput getThroughputJob(int clientId) {
        return new RunThroughput(clientId) {
            @Override
            public void warmupQuery() {
                int idx = rand.nextInt(assocRange_warmup);
                AssocRange.this.warmupQuery(g, idx);
            }

            @Override
            public int query() {
                int idx = rand.nextInt(assocRange_query);
                return AssocRange.this.query(g, idx).size();
            }
        };
    }

    static void readAssocRangeQueries(
            String file, long[] nodes, int[] atypes,
            int[] offsets, int[] lengths) {

        try {
            BufferedReader br = new BufferedReader(new FileReader(queryPath + "/" + file));
            for (int i = 0; i < nodes.length; i++) {
                String line = br.readLine();
                int idx = line.indexOf(',');
                nodes[i] = (Long.parseLong(line.substring(0, idx)));

                int idx2 = line.indexOf(',', idx + 1);
                atypes[i] = (Integer.parseInt(line.substring(idx + 1, idx2)));

                int idx3 = line.indexOf(',', idx2 + 1);
                offsets[i] = (Integer.parseInt(line.substring(idx2 + 1, idx3)));

                lengths[i] = (Integer.parseInt(line.substring(idx3 + 1)));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
