package coprocessor;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.io.FSDataInputStreamWrapper;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.regionserver.*;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.WALKey;

import coprocessor.generated.ObserverStatisticsProtos;

@SuppressWarnings("deprecation") // because of API usage
public class ObserverStatisticsEndpoint
        extends ObserverStatisticsProtos.ObserverStatisticsService
        implements Coprocessor, CoprocessorService, RegionObserver {

    private static final byte[] dataColF = Bytes.toBytes("data");
    private static final byte[] lastValueCol = Bytes.toBytes("last_value");

    private static final byte[] count = Bytes.toBytes("count");
    private static final byte[] avg = Bytes.toBytes("avg");
    private static final byte[] min = Bytes.toBytes("min");
    private static final byte[] max = Bytes.toBytes("max");

    private RegionCoprocessorEnvironment env;
    private Map<String, Integer> stats = new LinkedHashMap<>();
    private AtomicInteger c = new AtomicInteger();

    // Lifecycle methods

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        if (env instanceof RegionCoprocessorEnvironment) {
            this.env = (RegionCoprocessorEnvironment) env;
        } else {
            throw new CoprocessorException("Must be loaded on a table region!");
        }
    }

    @Override
    public void stop(CoprocessorEnvironment env) throws IOException {
    }

    @Override
    public Service getService() {
        return this;
    }


    @Override
    public void getStatistics(RpcController controller,
                              ObserverStatisticsProtos.StatisticsRequest request,
                              RpcCallback<ObserverStatisticsProtos.StatisticsResponse> done) {
        ObserverStatisticsProtos.StatisticsResponse response = null;
        try {
            ObserverStatisticsProtos.StatisticsResponse.Builder builder =
                    ObserverStatisticsProtos.StatisticsResponse.newBuilder();
            ObserverStatisticsProtos.NameInt32Pair.Builder pair =
                    ObserverStatisticsProtos.NameInt32Pair.newBuilder();
            for (Map.Entry<String, Integer> entry : stats.entrySet()) {
                pair.setName(entry.getKey());
                pair.setValue(entry.getValue());
                builder.addAttribute(pair.build());
            }
            response = builder.build();
            // optionally clear out stats
            if (request.hasClear() && request.getClear()) {
                synchronized (stats) {
                    stats.clear();
                }
            }
        } catch (Exception e) {
            ResponseConverter.setControllerException(controller,
                    new IOException(e));
        }
        done.run(response);
    }

    /**
     * Internal helper to keep track of call counts.
     *
     * @param call The name of the call.
     */
    private void addCallCount(String call) {
        synchronized (stats) {
            Integer count = stats.get(call);
            if (count == null) count = new Integer(1);
            else count = new Integer(count + 1);
            stats.put(call, count);
        }
    }

    // All Observer callbacks follow here

    @Override
    public void preOpen(
            ObserverContext<RegionCoprocessorEnvironment> observerContext)
            throws IOException {
        addCallCount("preOpen");
    }

    @Override
    public void postOpen(
            ObserverContext<RegionCoprocessorEnvironment> observerContext) {
        addCallCount("postOpen");
    }

    /*...*/
    // ^^ ObserverStatisticsEndpoint
    @Override
    public void postLogReplay(
            ObserverContext<RegionCoprocessorEnvironment> observerContext) {
        addCallCount("postLogReplay");
    }

    @Override
    public InternalScanner preFlushScannerOpen(
            ObserverContext<RegionCoprocessorEnvironment> observerContext, Store store,
            KeyValueScanner keyValueScanner, InternalScanner internalScanner)
            throws IOException {
        addCallCount("preFlushScannerOpen");
        return internalScanner;
    }

    @Override
    public void preFlush(
            ObserverContext<RegionCoprocessorEnvironment> observerContext)
            throws IOException {
        addCallCount("preFlush1");
    }

    @Override
    public InternalScanner preFlush(
            ObserverContext<RegionCoprocessorEnvironment> observerContext, Store store,
            InternalScanner internalScanner) throws IOException {
        addCallCount("preFlush2");
        return internalScanner;
    }

    @Override
    public void postFlush(
            ObserverContext<RegionCoprocessorEnvironment> observerContext)
            throws IOException {
        addCallCount("postFlush1");
    }

    @Override
    public void postFlush(
            ObserverContext<RegionCoprocessorEnvironment> observerContext, Store store,
            StoreFile storeFile) throws IOException {
        addCallCount("postFlush2");
    }

    @Override
    public void preCompactSelection(
            ObserverContext<RegionCoprocessorEnvironment> observerContext, Store store,
            List<StoreFile> list, CompactionRequest compactionRequest)
            throws IOException {
        addCallCount("preCompactSelection1");
    }

    @Override
    public void preCompactSelection(
            ObserverContext<RegionCoprocessorEnvironment> observerContext, Store store,
            List<StoreFile> list) throws IOException {
        addCallCount("preCompactSelection2");
    }

    @Override
    public void postCompactSelection(
            ObserverContext<RegionCoprocessorEnvironment> observerContext, Store store,
            ImmutableList<StoreFile> immutableList,
            CompactionRequest compactionRequest) {
        addCallCount("postCompactSelection1");
    }

    @Override
    public void postCompactSelection(
            ObserverContext<RegionCoprocessorEnvironment> observerContext, Store store,
            ImmutableList<StoreFile> immutableList) {
        addCallCount("postCompactSelection2");
    }

    @Override
    public InternalScanner preCompact(
            ObserverContext<RegionCoprocessorEnvironment> observerContext, Store store,
            InternalScanner internalScanner, ScanType scanType,
            CompactionRequest compactionRequest) throws IOException {
        addCallCount("preCompact1");
        return internalScanner;
    }

    @Override
    public InternalScanner preCompact(
            ObserverContext<RegionCoprocessorEnvironment> observerContext, Store store,
            InternalScanner internalScanner, ScanType scanType) throws IOException {
        addCallCount("preCompact2");
        return internalScanner;
    }

    @Override
    public InternalScanner preCompactScannerOpen(
            ObserverContext<RegionCoprocessorEnvironment> observerContext, Store store,
            List<? extends KeyValueScanner> list, ScanType scanType, long l,
            InternalScanner internalScanner, CompactionRequest compactionRequest)
            throws IOException {
        addCallCount("preCompactScannerOpen1");
        return internalScanner;
    }

    @Override
    public InternalScanner preCompactScannerOpen(
            ObserverContext<RegionCoprocessorEnvironment> observerContext, Store store,
            List<? extends KeyValueScanner> list, ScanType scanType, long l,
            InternalScanner internalScanner) throws IOException {
        addCallCount("preCompactScannerOpen2");
        return internalScanner;
    }

    @Override
    public void postCompact(
            ObserverContext<RegionCoprocessorEnvironment> observerContext, Store store,
            StoreFile storeFile, CompactionRequest compactionRequest)
            throws IOException {
        addCallCount("postCompact1");
    }

    @Override
    public void postCompact(
            ObserverContext<RegionCoprocessorEnvironment> observerContext, Store store,
            StoreFile storeFile) throws IOException {
        addCallCount("postCompact2");
    }

    @Override
    public void preSplit(
            ObserverContext<RegionCoprocessorEnvironment> observerContext)
            throws IOException {
        addCallCount("preSplit1");
    }

    @Override
    public void preSplit(
            ObserverContext<RegionCoprocessorEnvironment> observerContext, byte[] bytes)
            throws IOException {
        addCallCount("preSplit2");
    }

    @Override
    public void postSplit(
            ObserverContext<RegionCoprocessorEnvironment> observerContext,
            Region region, Region region1) throws IOException {
        addCallCount("postSplit");
    }

    @Override
    public void preSplitBeforePONR(
            ObserverContext<RegionCoprocessorEnvironment> observerContext, byte[] bytes,
            List<Mutation> list) throws IOException {
        addCallCount("preSplitBeforePONR");
    }

    @Override
    public void preSplitAfterPONR(
            ObserverContext<RegionCoprocessorEnvironment> observerContext)
            throws IOException {
        addCallCount("preSplitAfterPONR");
    }

    @Override
    public void preRollBackSplit(
            ObserverContext<RegionCoprocessorEnvironment> observerContext)
            throws IOException {
        addCallCount("preRollBackSplit");
    }

    @Override
    public void postRollBackSplit(
            ObserverContext<RegionCoprocessorEnvironment> observerContext)
            throws IOException {
        addCallCount("postRollBackSplit");
    }

    @Override
    public void postCompleteSplit(
            ObserverContext<RegionCoprocessorEnvironment> observerContext)
            throws IOException {
        addCallCount("postCompleteSplit");
    }

    @Override
    public void preClose(
            ObserverContext<RegionCoprocessorEnvironment> observerContext, boolean b)
            throws IOException {
        addCallCount("preClose");
    }

    @Override
    public void postClose(
            ObserverContext<RegionCoprocessorEnvironment> observerContext, boolean b) {
        addCallCount("postClose");
    }

    @Override
    public void preGetClosestRowBefore(
            ObserverContext<RegionCoprocessorEnvironment> observerContext, byte[] bytes,
            byte[] bytes1, Result result) throws IOException {
        addCallCount("preGetClosestRowBefore");
    }

    @Override
    public void postGetClosestRowBefore(
            ObserverContext<RegionCoprocessorEnvironment> observerContext, byte[] bytes,
            byte[] bytes1, Result result) throws IOException {
        addCallCount("postGetClosestRowBefore");
    }

    @Override
    public void preGetOp(
            ObserverContext<RegionCoprocessorEnvironment> observerContext, Get get,
            List<Cell> list) throws IOException {
        addCallCount("preGetOp");
    }

    @Override
    public void postGetOp(
            ObserverContext<RegionCoprocessorEnvironment> observerContext, Get get,
            List<Cell> list) throws IOException {
        addCallCount("postGetOp");
    }

    @Override
    public boolean preExists(
            ObserverContext<RegionCoprocessorEnvironment> observerContext, Get get,
            boolean b) throws IOException {
        addCallCount("preExists");
        return b;
    }

    @Override
    public boolean postExists(
            ObserverContext<RegionCoprocessorEnvironment> observerContext, Get get,
            boolean b) throws IOException {
        addCallCount("postExists");
        return b;
    }

    @Override
    public void prePut(
            final ObserverContext<RegionCoprocessorEnvironment> e, final Put originalPut,
            final WALEdit walEdit, final Durability durability) throws IOException {

        try {
            Region region = e.getEnvironment().getRegion();

            final byte[] row = originalPut.getRow();
            region.processRowsWithLocks(new StatisticRowMutationProcessor(Collections.singletonList(row)) {

                private long getValue(Result result, byte[] col) {
                    return Bytes.toLong(result.getValue(dataColF, col));
                }

                @Override
                public void process(long now,
                                    HRegion region,
                                    List<Mutation> mutationsToApply,
                                    WALEdit walEdit) throws IOException {
                    byte[] byteNow = Bytes.toBytes(now);

                    List<Mutation> mutations = doProcess(region);

                    for (Mutation m : mutations) {
                        if (m instanceof Put) {
                            Map<byte[], List<Cell>> familyMap = m.getFamilyCellMap();
                            region.checkFamilies(familyMap.keySet());
                            region.checkTimestamps(familyMap, now);
                            region.updateCellTimestamps(familyMap.values(), byteNow);
                        } else if (m instanceof Delete) {
                            Delete d = (Delete) m;
                            region.prepareDelete(d);
                            region.prepareDeleteTimestamps(d, d.getFamilyCellMap(), byteNow);
                        } else {
                            throw new DoNotRetryIOException("Action must be Put or Delete. But was: "
                                    + m.getClass().getName());
                        }
                        mutationsToApply.add(m);
                    }
                    for (Mutation m : mutations) {
                        for (List<Cell> cells : m.getFamilyCellMap().values()) {
                            boolean writeToWAL = m.getDurability() != Durability.SKIP_WAL;
                            for (Cell cell : cells) {
                                if (writeToWAL) walEdit.add(cell);
                            }
                        }
                    }

                }

                private List<Mutation> doProcess(HRegion region) throws IOException {
                    List<Mutation> mutations = new ArrayList<>();

                    Put newPut = new Put(row);

                    List<Cell> originalPutCells = originalPut.get(dataColF, lastValueCol);

                    if (!originalPutCells.isEmpty()) {
                        long originalValue = Bytes.toLong(CellUtil.cloneValue(originalPutCells.get(0)));
                        byte[] originalValueBytes = Bytes.toBytes(originalValue);
                        addCallCount("originalValue was:" + originalValue);

                        Get get = new Get(row);
                        get.addColumn(dataColF, count);
                        get.addColumn(dataColF, min);
                        get.addColumn(dataColF, max);
                        get.addColumn(dataColF, avg);

                        Result result = region.get(get);
                        addCallCount("result empty: " + result.isEmpty());
                        if (!result.isEmpty()) {
                            long countVal = getValue(result, count);
                            long minVal = getValue(result, min);
                            long maxVal = getValue(result, max);
                            long avgVal = getValue(result, avg);

                            addCallCount("count was:" + countVal);
                            addCallCount("minVal was:" + minVal);
                            addCallCount("maxVal was:" + maxVal);
                            addCallCount("avgVal was:" + avgVal);

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

        } catch (Exception ex) {
            writeEx(ex);
        }
        addCallCount("prePut");
    }

    private void writeEx(Exception ex) {
        addCallCount(ex.getMessage());
        StringWriter out = new StringWriter();
        ex.printStackTrace(new PrintWriter(out));
        addCallCount(out.getBuffer().toString());
    }

    private Put getPut(String row, byte[] colf, byte[] col, String val) {
        Put p1 = new Put(Bytes.toBytes(row));
        p1.addColumn(colf, col, Bytes.toBytes(val));
        return p1;
    }

        @Override
    public void postPut(
            ObserverContext<RegionCoprocessorEnvironment> observerContext, Put put,
            WALEdit walEdit, Durability durability) throws IOException {
        addCallCount("postPut");
    }

    @Override
    public void preDelete(
            ObserverContext<RegionCoprocessorEnvironment> observerContext,
            Delete delete, WALEdit walEdit, Durability durability) throws IOException {
        addCallCount("preDelete");
    }

    @Override
    public void prePrepareTimeStampForDeleteVersion(
            ObserverContext<RegionCoprocessorEnvironment> observerContext,
            Mutation mutation, Cell cell, byte[] bytes, Get get) throws IOException {
        addCallCount("prePrepareTimeStampForDeleteVersion");
    }

    @Override
    public void postDelete(
            ObserverContext<RegionCoprocessorEnvironment> observerContext,
            Delete delete, WALEdit walEdit, Durability durability) throws IOException {
        addCallCount("postDelete");
    }

    @Override
    public void preBatchMutate(
            ObserverContext<RegionCoprocessorEnvironment> observerContext,
            MiniBatchOperationInProgress<Mutation> miniBatchOperationInProgress)
            throws IOException {
        addCallCount("preBatchMutate");
    }

    @Override
    public void postBatchMutate(
            ObserverContext<RegionCoprocessorEnvironment> observerContext,
            MiniBatchOperationInProgress<Mutation> miniBatchOperationInProgress)
            throws IOException {
        addCallCount("postBatchMutate");
    }

    @Override
    public void postStartRegionOperation(
            ObserverContext<RegionCoprocessorEnvironment> observerContext,
            HRegion.Operation operation) throws IOException {
        addCallCount("postStartRegionOperation");
        addCallCount("- postStartRegionOperation-" + operation);
    }

    @Override
    public void postCloseRegionOperation(
            ObserverContext<RegionCoprocessorEnvironment> observerContext,
            HRegion.Operation operation) throws IOException {
        addCallCount("postCloseRegionOperation");
        addCallCount("- postCloseRegionOperation-" + operation);
    }

    @Override
    public void postBatchMutateIndispensably(
            ObserverContext<RegionCoprocessorEnvironment> observerContext,
            MiniBatchOperationInProgress<Mutation> miniBatchOperationInProgress,
            boolean b) throws IOException {
        addCallCount("postBatchMutateIndispensably");
    }

    @Override
    public boolean preCheckAndPut(
            ObserverContext<RegionCoprocessorEnvironment> observerContext, byte[] bytes,
            byte[] bytes1, byte[] bytes2, CompareFilter.CompareOp compareOp,
            ByteArrayComparable byteArrayComparable, Put put, boolean b)
            throws IOException {
        addCallCount("preCheckAndPut");
        return b;
    }

    @Override
    public boolean preCheckAndPutAfterRowLock(
            ObserverContext<RegionCoprocessorEnvironment> e, byte[] bytes,
            byte[] bytes1, byte[] bytes2, CompareFilter.CompareOp compareOp,
            ByteArrayComparable byteArrayComparable, Put put, boolean b)
            throws IOException {
//    addCallCount("preCheckAndPutAfterRowLock");

//    byte[] row = put.getRow();
//    Cell cell = put.get(Bytes.toBytes("cf1"), Bytes.toBytes("value")).get(0);
//    String value = Bytes.toString(CellUtil.cloneValue(cell));
//    String rowVal = Bytes.toString(row);
//
//    String prePut = String.format("%s time pre put, row: %s, value: %s", c.incrementAndGet(), rowVal, value);

        return b;
    }

    @Override
    public boolean postCheckAndPut(
            ObserverContext<RegionCoprocessorEnvironment> observerContext, byte[] bytes,
            byte[] bytes1, byte[] bytes2, CompareFilter.CompareOp compareOp,
            ByteArrayComparable byteArrayComparable, Put put, boolean b)
            throws IOException {
        addCallCount("postCheckAndPut");
        return b;
    }

    @Override
    public boolean preCheckAndDelete(
            ObserverContext<RegionCoprocessorEnvironment> observerContext, byte[] bytes,
            byte[] bytes1, byte[] bytes2, CompareFilter.CompareOp compareOp,
            ByteArrayComparable byteArrayComparable, Delete delete, boolean b)
            throws IOException {
        addCallCount("preCheckAndDelete");
        return b;
    }

    @Override
    public boolean preCheckAndDeleteAfterRowLock(
            ObserverContext<RegionCoprocessorEnvironment> observerContext, byte[] bytes,
            byte[] bytes1, byte[] bytes2, CompareFilter.CompareOp compareOp,
            ByteArrayComparable byteArrayComparable, Delete delete, boolean b)
            throws IOException {
        addCallCount("preCheckAndDeleteAfterRowLock");
        return b;
    }

    @Override
    public boolean postCheckAndDelete(
            ObserverContext<RegionCoprocessorEnvironment> observerContext, byte[] bytes,
            byte[] bytes1, byte[] bytes2, CompareFilter.CompareOp compareOp,
            ByteArrayComparable byteArrayComparable, Delete delete, boolean b)
            throws IOException {
        addCallCount("postCheckAndDelete");
        return b;
    }

    @Override
    public long preIncrementColumnValue(
            ObserverContext<RegionCoprocessorEnvironment> observerContext, byte[] bytes,
            byte[] bytes1, byte[] bytes2, long l, boolean b) throws IOException {
        addCallCount("preIncrementColumnValue");
        return l;
    }

    @Override
    public long postIncrementColumnValue(
            ObserverContext<RegionCoprocessorEnvironment> observerContext, byte[] bytes,
            byte[] bytes1, byte[] bytes2, long l, boolean b, long l1)
            throws IOException {
        addCallCount("postIncrementColumnValue");
        return l;
    }

    @Override
    public Result preAppend(
            ObserverContext<RegionCoprocessorEnvironment> observerContext,
            Append append) throws IOException {
        addCallCount("preAppend");
        return null;
    }

    @Override
    public Result preAppendAfterRowLock(
            ObserverContext<RegionCoprocessorEnvironment> observerContext,
            Append append) throws IOException {
        addCallCount("preAppendAfterRowLock");
        return null;
    }

    @Override
    public Result postAppend(
            ObserverContext<RegionCoprocessorEnvironment> observerContext,
            Append append, Result result) throws IOException {
        addCallCount("postAppend");
        return result;
    }

    @Override
    public Result preIncrement(
            ObserverContext<RegionCoprocessorEnvironment> observerContext,
            Increment increment) throws IOException {
        addCallCount("preIncrement");
        return null;
    }

    @Override
    public Result preIncrementAfterRowLock(
            ObserverContext<RegionCoprocessorEnvironment> observerContext,
            Increment increment) throws IOException {

        addCallCount("preIncrementAfterRowLock");
        return null;
    }

    @Override
    public Result postIncrement(
            ObserverContext<RegionCoprocessorEnvironment> observerContext,
            Increment increment, Result result) throws IOException {
        addCallCount("postIncrement");
        return result;
    }

    @Override
    public RegionScanner preScannerOpen(
            ObserverContext<RegionCoprocessorEnvironment> observerContext, Scan scan,
            RegionScanner regionScanner) throws IOException {
        addCallCount("preScannerOpen");
        return regionScanner;
    }

    @Override
    public KeyValueScanner preStoreScannerOpen(
            ObserverContext<RegionCoprocessorEnvironment> observerContext, Store store,
            Scan scan, NavigableSet<byte[]> navigableSet,
            KeyValueScanner keyValueScanner) throws IOException {
        addCallCount("preStoreScannerOpen");
        return keyValueScanner;
    }

    @Override
    public RegionScanner postScannerOpen(
            ObserverContext<RegionCoprocessorEnvironment> observerContext, Scan scan,
            RegionScanner regionScanner) throws IOException {
        addCallCount("postScannerOpen");
        return regionScanner;
    }

    @Override
    public boolean preScannerNext(
            ObserverContext<RegionCoprocessorEnvironment> observerContext,
            InternalScanner internalScanner, List<Result> list, int i, boolean b)
            throws IOException {
        addCallCount("preScannerNext");
        return b;
    }

    @Override
    public boolean postScannerNext(
            ObserverContext<RegionCoprocessorEnvironment> observerContext,
            InternalScanner internalScanner, List<Result> list, int i, boolean b)
            throws IOException {
        addCallCount("postScannerNext");
        return b;
    }

    @Override
    public boolean postScannerFilterRow(
            ObserverContext<RegionCoprocessorEnvironment> observerContext,
            InternalScanner internalScanner, byte[] bytes, int i, short i1, boolean b)
            throws IOException {
        addCallCount("postScannerFilterRow");
        return b;
    }

    @Override
    public void preScannerClose(
            ObserverContext<RegionCoprocessorEnvironment> observerContext,
            InternalScanner internalScanner) throws IOException {
        addCallCount("preScannerClose");
    }

    @Override
    public void postScannerClose(
            ObserverContext<RegionCoprocessorEnvironment> observerContext,
            InternalScanner internalScanner) throws IOException {
        addCallCount("postScannerClose");
    }

    @Override
    public void preWALRestore(
            ObserverContext<? extends RegionCoprocessorEnvironment> observerContext,
            HRegionInfo hRegionInfo, WALKey walKey, WALEdit walEdit)
            throws IOException {
        addCallCount("preWALRestore1");
    }

    @Override
    public void preWALRestore(
            ObserverContext<RegionCoprocessorEnvironment> observerContext,
            HRegionInfo hRegionInfo, HLogKey hLogKey, WALEdit walEdit)
            throws IOException {
        addCallCount("preWALRestore2");
    }

    @Override
    public void postWALRestore(
            ObserverContext<? extends RegionCoprocessorEnvironment> observerContext,
            HRegionInfo hRegionInfo, WALKey walKey, WALEdit walEdit)
            throws IOException {
        addCallCount("postWALRestore1");
    }

    @Override
    public void postWALRestore(
            ObserverContext<RegionCoprocessorEnvironment> observerContext,
            HRegionInfo hRegionInfo, HLogKey hLogKey, WALEdit walEdit)
            throws IOException {
        addCallCount("postWALRestore2");
    }

    @Override
    public void preBulkLoadHFile(
            ObserverContext<RegionCoprocessorEnvironment> observerContext,
            List<Pair<byte[], String>> list) throws IOException {
        addCallCount("preBulkLoadHFile");
    }

    @Override
    public boolean postBulkLoadHFile(
            ObserverContext<RegionCoprocessorEnvironment> observerContext,
            List<Pair<byte[], String>> list, boolean b) throws IOException {
        addCallCount("postBulkLoadHFile");
        return b;
    }

    @Override
    public StoreFile.Reader preStoreFileReaderOpen(
            ObserverContext<RegionCoprocessorEnvironment> observerContext,
            FileSystem fileSystem, Path path,
            FSDataInputStreamWrapper fsDataInputStreamWrapper, long l,
            CacheConfig cacheConfig, Reference reference, StoreFile.Reader reader)
            throws IOException {
        addCallCount("preStoreFileReaderOpen");
        addCallCount("- preStoreFileReaderOpen-" + path.getName());
        return reader;
    }

    @Override
    public StoreFile.Reader postStoreFileReaderOpen(
            ObserverContext<RegionCoprocessorEnvironment> observerContext,
            FileSystem fileSystem, Path path,
            FSDataInputStreamWrapper fsDataInputStreamWrapper, long l,
            CacheConfig cacheConfig, Reference reference, StoreFile.Reader reader)
            throws IOException {
        addCallCount("postStoreFileReaderOpen");
        addCallCount("- postStoreFileReaderOpen-" + path.getName());
        return reader;
    }

    @Override
    public Cell postMutationBeforeWAL(
            ObserverContext<RegionCoprocessorEnvironment> observerContext,
            MutationType mutationType, Mutation mutation, Cell cell, Cell cell1)
            throws IOException {
        addCallCount("postMutationBeforeWAL");
        addCallCount("- postMutationBeforeWAL-" + mutationType);

        return cell;
    }

    @Override
    public DeleteTracker postInstantiateDeleteTracker(
            ObserverContext<RegionCoprocessorEnvironment> observerContext,
            DeleteTracker deleteTracker) throws IOException {
        addCallCount("postInstantiateDeleteTracker");
        return deleteTracker;
    }
    // vv ObserverStatisticsEndpoint
}
// ^^ ObserverStatisticsEndpoint
