package hbase;

import hbase.domain.Event;
import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by cloudera on 10/20/16.
 */
public class EventProcessorImpl implements EventProcessor {

    private static final TableName tableName = TableName.valueOf("stats");
    private static final byte[] dataColF = Bytes.toBytes("data");
    private static final byte[] lastValueCol = Bytes.toBytes("last_value");

    private final Configuration conf;

    public EventProcessorImpl(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public void accept(Event event) throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(conf)) {

            Table table = connection.getTable(tableName);

            long time = DateUtils.truncate(new Date(event.getTimestamp()), Calendar.MINUTE).getTime();

            byte[] row = Bytes.toBytes(time);
            Put put = new Put(row);
            put.addColumn(dataColF, lastValueCol, Bytes.toBytes(event.getValue()));

            table.put(put);
        }
    }
}
