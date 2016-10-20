package coprocessor;


import com.google.protobuf.Message;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.protobuf.generated.MultiRowMutationProtos.MultiRowMutationProcessorRequest;
import org.apache.hadoop.hbase.regionserver.BaseRowProcessor;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

public abstract class StatisticRowMutationProcessor extends BaseRowProcessor<Message, Message> {

    private Collection<byte[]> rowsToLock;

    public StatisticRowMutationProcessor(Collection<byte[]> rowsToLock) {
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
    public abstract void process(long now,
                        HRegion region,
                        List<Mutation> mutationsToApply,
                        WALEdit walEdit) throws IOException;


    @Override
    public MultiRowMutationProcessorRequest getRequestData() {
        return null;
    }

    @Override
    public void initialize(Message msg) {
    }
}

