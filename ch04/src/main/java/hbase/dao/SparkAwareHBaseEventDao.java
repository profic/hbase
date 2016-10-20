package hbase.dao;

import hbase.domain.StatisticsEntry;
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
public class SparkAwareHBaseEventDao {

    private final Configuration conf;

    private static final TableName tableName = TableName.valueOf("stats");
    private static final byte[] dataColF = Bytes.toBytes("data");
    private static final byte[] count = Bytes.toBytes("count");
    private static final byte[] avg = Bytes.toBytes("avg");
    private static final byte[] min = Bytes.toBytes("min");
    private static final byte[] max = Bytes.toBytes("max");

    public SparkAwareHBaseEventDao(Configuration conf) {
        this.conf = conf;
    }

    public void save(StatisticsEntry event) throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(conf);
             Table table = connection.getTable(tableName)) {


            long time = DateUtils.truncate(new Date(event.getTimestamp()), Calendar.MINUTE).getTime();

            byte[] row = Bytes.toBytes(time);
            Put put = new Put(row);
            put.addColumn(dataColF, count, Bytes.toBytes(event.getCount()));
            put.addColumn(dataColF, avg, Bytes.toBytes(event.getAvg()));
            put.addColumn(dataColF, max, Bytes.toBytes(event.getMax()));
            put.addColumn(dataColF, min, Bytes.toBytes(event.getMin()));

            table.put(put);
        }
    }
}
