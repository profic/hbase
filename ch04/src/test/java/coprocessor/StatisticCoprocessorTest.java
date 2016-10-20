package coprocessor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * Created by cloudera on 10/20/16.
 */
public class StatisticCoprocessorTest {

    private static final byte[] dataColF = Bytes.toBytes("data");
    private static final byte[] lastValueCol = Bytes.toBytes("last_value");
    private static final byte[] count = Bytes.toBytes("count");
    private static final byte[] avg = Bytes.toBytes("avg");
    private static final byte[] min = Bytes.toBytes("min");
    private static final byte[] max = Bytes.toBytes("max");

    private static final TableName tableName = TableName.valueOf("stats");

    @Before
    public void before() throws Exception {
        truncateTable();
    }

    @AfterClass
    public static void afterClass() throws Exception {
        truncateTable();
    }

    @Test
    public void shouldInsertCorrectAvg() throws Exception {

        Configuration conf = HBaseConfiguration.create();
        try (Connection connection = ConnectionFactory.createConnection(conf)) {
            Table table = connection.getTable(tableName);

            byte[] row = Bytes.toBytes(1L);

            Put p1 = new Put(row);
            p1.addColumn(dataColF, lastValueCol, Bytes.toBytes(2L));

            Put p2 = new Put(row);
            p2.addColumn(dataColF, lastValueCol, Bytes.toBytes(4L));

            Put p3 = new Put(row);
            p3.addColumn(dataColF, lastValueCol, Bytes.toBytes(9L));

            table.put(p1);
            table.put(p2);
            table.put(p3);

            Result result = getAllColumns(table, row);

            double avgVal = Bytes.toDouble(result.getValue(dataColF, avg));

            assertEquals(5, avgVal, 0.1);
        }
    }

    @Test
    public void shouldInsertCorrectMax() throws Exception {

        Configuration conf = HBaseConfiguration.create();
        try (Connection connection = ConnectionFactory.createConnection(conf)) {
            Table table = connection.getTable(tableName);

            byte[] row = Bytes.toBytes(1L);

            Put p1 = new Put(row);
            p1.addColumn(dataColF, lastValueCol, Bytes.toBytes(2L));

            Put p2 = new Put(row);
            p2.addColumn(dataColF, lastValueCol, Bytes.toBytes(4L));

            Put p3 = new Put(row);
            p3.addColumn(dataColF, lastValueCol, Bytes.toBytes(9L));

            table.put(p1);
            table.put(p2);
            table.put(p3);

            Result result = getAllColumns(table, row);
            long maxVal = getValue(result, max);
            assertEquals(9L, maxVal);
        }
    }

    @Test
    public void shouldInsertCorrectMin() throws Exception {

        Configuration conf = HBaseConfiguration.create();
        try (Connection connection = ConnectionFactory.createConnection(conf)) {
            Table table = connection.getTable(tableName);

            byte[] row = Bytes.toBytes(1L);

            Put p1 = new Put(row);
            p1.addColumn(dataColF, lastValueCol, Bytes.toBytes(9L));

            Put p2 = new Put(row);
            p2.addColumn(dataColF, lastValueCol, Bytes.toBytes(4L));

            Put p3 = new Put(row);
            p3.addColumn(dataColF, lastValueCol, Bytes.toBytes(2L));

            table.put(p1);
            table.put(p2);
            table.put(p3);

            Result result = getAllColumns(table, row);
            long minVal = getValue(result, min);
            assertEquals(2L, minVal);
        }
    }

    @Test
    public void shouldReturnCorrectCount() throws Exception {

        Configuration conf = HBaseConfiguration.create();
        try (Connection connection = ConnectionFactory.createConnection(conf)) {
            Table table = connection.getTable(tableName);

            byte[] row = Bytes.toBytes(1L);

            Put p1 = new Put(row);
            p1.addColumn(dataColF, lastValueCol, Bytes.toBytes(2L));

            Put p2 = new Put(row);
            p2.addColumn(dataColF, lastValueCol, Bytes.toBytes(4L));

            Put p3 = new Put(row);
            p3.addColumn(dataColF, lastValueCol, Bytes.toBytes(9L));

            table.put(p1);
            table.put(p2);
            table.put(p3);

            Result result = getAllColumns(table, row);
            long countVal = getValue(result, count);

            assertEquals(3, countVal);
        }
    }

    private Result getAllColumns(Table table, byte[] row) throws IOException {
        Get get = new Get(row);
        get.addColumn(dataColF, count);
        get.addColumn(dataColF, min);
        get.addColumn(dataColF, max);
        get.addColumn(dataColF, avg);

        return table.get(get);
    }

    private static long getValue(Result result, byte[] col) {
        return Bytes.toLong(result.getValue(dataColF, col));
    }

    private static void truncateTable() throws IOException {
        Configuration conf = HBaseConfiguration.create();

        try (Connection connection = ConnectionFactory.createConnection(conf)) {
            Admin admin = connection.getAdmin();
            admin.disableTable(tableName);
            admin.truncateTable(tableName, false);
        }
    }

}
