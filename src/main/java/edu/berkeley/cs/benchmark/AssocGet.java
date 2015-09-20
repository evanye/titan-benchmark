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
                modGet(warmupAssocGetNodes, i),
                modGet(warmupAssocGetAtypes, i),
                modGet(warmupAssocGetDstIdSets, i),
                modGet(warmupAssocGetTimeLows, i),
                modGet(warmupAssocGetTimeHighs, i));
    }

    @Override
    public List<Assoc> query(Graph g, int i) {
        return g.assocGet(
                modGet(assocGetNodes, i),
                modGet(assocGetAtypes, i),
                modGet(assocGetDstIdSets, i),
                modGet(assocGetTimeLows, i),
                modGet(assocGetTimeHighs, i));
    }

    static void readAssocGetQueries(
            String file, List<Long> nodes, List<Integer> atypes,
            List<Set<Long>> dstIdSets, List<Long> tLows, List<Long> tHighs) {

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

                if (idx4 == -1) {
                    tHighs.add(Long.parseLong(line.substring(idx3 + 1)));
                    dstIdSets.add(new HashSet<Long>());
                    line = br.readLine();
                    continue;
                }
                tHighs.add(Long.parseLong(line.substring(idx3 + 1, idx4)));

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
                line = br.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
