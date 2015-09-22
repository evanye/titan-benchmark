package edu.berkeley.cs.benchmark;

import edu.berkeley.cs.titan.Assoc;
import edu.berkeley.cs.titan.Graph;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class AssocGet extends Benchmark<List<Assoc>> {
    public static final String WARMUP_FILE = "assocGet_warmup.txt";
    public static final String QUERY_FILE = "assocGet_query.txt";

    @Override
    public void readQueries() {
        readAssocGetQueries(WARMUP_FILE,
                warmupAssocGetNodes, warmupAssocGetAtypes,
                warmupAssocGetDstIdSets, warmupAssocGetTimeLows,
                warmupAssocGetTimeHighs);

        readAssocGetQueries(QUERY_FILE,
                assocGetNodes, assocGetAtypes,
                assocGetDstIdSets, assocGetTimeLows, assocGetTimeHighs);
    }

    @Override
    public List<Assoc> warmupQuery(Graph g, int i) {
        return g.assocGet(
                warmupAssocGetNodes[i],
                warmupAssocGetAtypes[i],
                warmupAssocGetDstIdSets.get(i),
                warmupAssocGetTimeLows[i],
                warmupAssocGetTimeHighs[i]);
    }

    @Override
    public List<Assoc> query(Graph g, int i) {
        return g.assocGet(
                assocGetNodes[i],
                assocGetAtypes[i],
                assocGetDstIdSets.get(i),
                assocGetTimeLows[i],
                assocGetTimeHighs[i]);
    }

    static void readAssocGetQueries(
            String file, long[] nodes, int[] atypes,
            List<Set<Long>> dstIdSets, long[] tLows, long[] tHighs) {

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

                if (idx4 == -1) {
                    tHighs[i] = (Long.parseLong(line.substring(idx3 + 1)));
                    dstIdSets.add(new HashSet<Long>());
                }
                tHighs[i] = (Long.parseLong(line.substring(idx3 + 1, idx4)));

                int idxLast = idx4, idxCurr;
                Set<Long> dstIdSet = new HashSet<>();
                while (true) {
                    idxCurr = line.indexOf(',', idxLast + 1);
                    if (idxCurr == -1) {
                        break;
                    }
                    dstIdSet.add(Long.parseLong(
                            line.substring(idxLast + 1, idxCurr)));
                    idxLast = idxCurr;
                }
                dstIdSet.add(Long.parseLong(line.substring(idxLast + 1)));
                dstIdSets.add(dstIdSet);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
