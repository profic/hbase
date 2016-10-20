package coprocessor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class ObserverStatisticsExample {

  /*
mvn clean
mvn -Djar.finalName=31 install

sudo -u hdfs hadoop fs -rm /31.jar
sudo -u hdfs hadoop fs -put /home/cloudera/Downloads/hbase-book-master/ch04/target/31.jar /


disable 'stats'
drop 'stats'
create 'stats', 'data'
alter 'stats', 'Coprocessor' => '/31.jar|coprocessor.ObserverStatisticsEndpoint|'

   */
    private static Table table = null;

    private static final byte[] dataColF = Bytes.toBytes("data");

    private static long getValue(Result result, byte[] col) {
        return Bytes.toLong(result.getValue(dataColF, col));
    }

    public static void main(String[] args) throws IOException {

        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);

        try {

            final byte[] colf = Bytes.toBytes("data");


            TableName tableName = TableName.valueOf("stats");
            table = connection.getTable(tableName);

            byte[] row = Bytes.toBytes(1L);

//            getAndPringRows(row);

            Put p1 = new Put(row);
            byte[] value = Bytes.toBytes("last_value");

            p1.addColumn(colf, value, Bytes.toBytes(20L));


            Put p2 = new Put(row);
            p2.addColumn(colf, value, Bytes.toBytes(30L));

            Put p3 = new Put(row);
            p3.addColumn(colf, value, Bytes.toBytes(5L));


            table.put(p1);
            table.put(p2);
            table.put(p3);


        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

    }

    private static void getAndPringRows(byte[] row) throws IOException {
        final byte[] count = Bytes.toBytes("count");
        final byte[] avg = Bytes.toBytes("avg");
        final byte[] min = Bytes.toBytes("min");
        final byte[] max = Bytes.toBytes("max");


        Get get = new Get(row);
        get.addColumn(dataColF, count);
        get.addColumn(dataColF, min);
        get.addColumn(dataColF, max);
        get.addColumn(dataColF, avg);

        Result result = table.get(get);

        long countVal = getValue(result, count);
        long minVal = getValue(result, min);
        long maxVal = getValue(result, max);
        double avgVal = Bytes.toDouble(result.getValue(dataColF, avg));

        System.out.println("countVal = " + countVal);
        System.out.println("minVal = " + minVal);
        System.out.println("maxVal = " + maxVal);
        System.out.println("avgVal = " + avgVal);
    }
}
