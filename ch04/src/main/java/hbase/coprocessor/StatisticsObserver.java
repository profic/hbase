package hbase.coprocessor;

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

public class StatisticsObserver extends BaseRegionObserver {

    private static final byte[] dataColF = Bytes.toBytes("data");
    private static final byte[] lastValueCol = Bytes.toBytes("last_value");

    private static final byte[] count = Bytes.toBytes("count");
    private static final byte[] avg = Bytes.toBytes("avg");
    private static final byte[] min = Bytes.toBytes("min");
    private static final byte[] max = Bytes.toBytes("max");

    @Override
    public void prePut(final ObserverContext<RegionCoprocessorEnvironment> e,
                       final Put originalPut,
                       final WALEdit walEdit,
                       final Durability durability) throws IOException {

        Region region = e.getEnvironment().getRegion();

        final byte[] row = originalPut.getRow();
        region.processRowsWithLocks(new MultipleRowMutationProcessor(Collections.singletonList(row)) {

            private long getValue(Result result, byte[] col) {
                return Bytes.toLong(result.getValue(dataColF, col));
            }

            @Override
            protected List<Mutation> doProcess(HRegion region) throws IOException {
                List<Mutation> mutations = new ArrayList<>();

                List<Cell> originalPutCells = originalPut.get(dataColF, lastValueCol);

                if (!originalPutCells.isEmpty()) {
                    long originalValue = Bytes.toLong(CellUtil.cloneValue(originalPutCells.get(0)));
                    byte[] originalValueBytes = Bytes.toBytes(originalValue);

                    long newCountVal;
                    double avgVal;

                    Result result = getPreviousValue(region, row);

                    if (result.isEmpty()) {
                        avgVal = originalValue;
                        newCountVal = 1L;

                        originalPut.addColumn(dataColF, min, originalValueBytes);
                        originalPut.addColumn(dataColF, max, originalValueBytes);
                    } else {
                        long countVal = getValue(result, count);
                        long minVal = getValue(result, min);
                        long maxVal = getValue(result, max);

                        avgVal = Bytes.toDouble(result.getValue(dataColF, avg));

                        if (originalValue < minVal) {
                            originalPut.addColumn(dataColF, min, originalValueBytes);
                        } else if (originalValue > maxVal) {
                            originalPut.addColumn(dataColF, max, originalValueBytes);
                        }
                        newCountVal = countVal + 1;
                    }

                    double calculatedAvg = calculateAvg(avgVal, originalValue, newCountVal);

                    originalPut.addColumn(dataColF, count, Bytes.toBytes(newCountVal));
                    originalPut.addColumn(dataColF, avg, Bytes.toBytes(calculatedAvg));

                    mutations.add(originalPut);

                    e.bypass();
                }
                return mutations;
            }
        });
    }

    private Result getPreviousValue(HRegion region, byte[] row) throws IOException {
        Get get = new Get(row);
        get.addColumn(dataColF, count);
        get.addColumn(dataColF, min);
        get.addColumn(dataColF, max);
        get.addColumn(dataColF, avg);
        return region.get(get);
    }

    private static double calculateAvg(double avg, double newValue, long count) {
        avg -= avg / count;
        avg += newValue / count;
        return avg;
    }
}
