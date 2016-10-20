package coprocessor;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@SuppressWarnings("deprecation") // because of API usage
public class ObserverStatisticsEndpoint extends BaseRegionObserver {

    private static final byte[] dataColF = Bytes.toBytes("data");
    private static final byte[] lastValueCol = Bytes.toBytes("last_value");

    private static final byte[] count = Bytes.toBytes("count");
    private static final byte[] avg = Bytes.toBytes("avg");
    private static final byte[] min = Bytes.toBytes("min");
    private static final byte[] max = Bytes.toBytes("max");

    @Override
    public void prePut(
            final ObserverContext<RegionCoprocessorEnvironment> e, final Put originalPut,
            final WALEdit walEdit, final Durability durability) throws IOException {

        Region region = e.getEnvironment().getRegion();

        final byte[] row = originalPut.getRow();
        region.processRowsWithLocks(new StatisticRowMutationProcessor(Collections.singletonList(row)) {

            private long getValue(Result result, byte[] col) {
                return Bytes.toLong(result.getValue(dataColF, col));
            }

            @Override
            protected List<Mutation> doProcess(HRegion region) throws IOException {
                List<Mutation> mutations = new ArrayList<>();

                Put newPut = new Put(row);

                List<Cell> originalPutCells = originalPut.get(dataColF, lastValueCol);

                if (!originalPutCells.isEmpty()) {
                    long originalValue = Bytes.toLong(CellUtil.cloneValue(originalPutCells.get(0)));
                    byte[] originalValueBytes = Bytes.toBytes(originalValue);

                    Get get = new Get(row);
                    get.addColumn(dataColF, count);
                    get.addColumn(dataColF, min);
                    get.addColumn(dataColF, max);
                    get.addColumn(dataColF, avg);

                    Result result = region.get(get);
                    if (!result.isEmpty()) {
                        long countVal = getValue(result, count);
                        long minVal = getValue(result, min);
                        long maxVal = getValue(result, max);
                        long avgVal = getValue(result, avg);

                        if (originalValue < minVal) {
                            newPut.addColumn(dataColF, min, originalValueBytes);
                        } else if (originalValue > maxVal) {
                            newPut.addColumn(dataColF, max, originalValueBytes);
                        }
                        newPut.addColumn(dataColF, count, Bytes.toBytes(countVal + 1));
                        newPut.addColumn(dataColF, avg, Bytes.toBytes(originalValue + avgVal));
                    } else {
                        newPut.addColumn(dataColF, min, originalValueBytes);
                        newPut.addColumn(dataColF, max, originalValueBytes);
                        newPut.addColumn(dataColF, count, Bytes.toBytes(1L));
                        newPut.addColumn(dataColF, avg, Bytes.toBytes(originalValue));
                    }

                    mutations.add(newPut);

                    e.bypass();
                }
                return mutations;
            }
        });
    }
}
