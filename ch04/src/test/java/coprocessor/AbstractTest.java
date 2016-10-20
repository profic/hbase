package coprocessor;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created by cloudera on 10/20/16.
 */
public abstract class AbstractTest {

    protected static final byte[] dataColF = Bytes.toBytes("data");
    protected static final byte[] lastValueCol = Bytes.toBytes("last_value");
    protected static final byte[] count = Bytes.toBytes("count");
    protected static final byte[] avg = Bytes.toBytes("avg");
    protected static final byte[] min = Bytes.toBytes("min");
    protected static final byte[] max = Bytes.toBytes("max");

    protected static final TableName tableName = TableName.valueOf("stats");

    protected Result getAllColumns(Table table, byte[] row) throws IOException {
        Get get = new Get(row);
        get.addColumn(dataColF, count);
        get.addColumn(dataColF, min);
        get.addColumn(dataColF, max);
        get.addColumn(dataColF, avg);
        return table.get(get);
    }

    protected static long getValue(Result result, byte[] col) {
        return Bytes.toLong(result.getValue(dataColF, col));
    }

}
