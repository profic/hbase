package hbase;


import com.google.protobuf.Message;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.regionserver.BaseRowProcessor;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public abstract class MultipleRowMutationProcessor extends BaseRowProcessor<Message, Message> {

    private Collection<byte[]> rowsToLock;

    public MultipleRowMutationProcessor(Collection<byte[]> rowsToLock) {
        this.rowsToLock = rowsToLock;
    }

    @Override
    public Collection<byte[]> getRowsToLock() {
        return rowsToLock;
    }

    @Override
    public boolean readOnly() {
        return false;
    }

    @Override
    public Message getResult() {
        return null;
    }

    @Override
    public void process(long now,
                        HRegion region,
                        List<Mutation> mutationsToApply,
                        WALEdit walEdit) throws IOException {
        List<Mutation> mutations = doProcess(region);

        byte[] byteNow = Bytes.toBytes(now);
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


    @Override
    public Message getRequestData() {
        return null;
    }

    @Override
    public void initialize(Message msg) {
    }

    protected abstract List<Mutation> doProcess(HRegion region) throws IOException;
}

