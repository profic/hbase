package coprocessor;

import java.io.IOException;
import java.util.*;

import coprocessor.generated.ObserverStatisticsProtos;
import coprocessor.generated.ObserverStatisticsProtos.NameInt32Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.util.Bytes;

import util.HBaseHelper;

import static coprocessor.generated.ObserverStatisticsProtos.*;

// cc ObserverStatisticsExample Use an endpoint to query observer statistics
public class ObserverStatisticsExample {

    // vv ObserverStatisticsExample
    private static Table table = null;

  /*
  mvn clean install

mvn clean
mvn -Djar.finalName=11 install

sudo -u hdfs hadoop fs -rm /11.jar
sudo -u hdfs hadoop fs -put /home/cloudera/Downloads/hbase-book-master/ch04/target/11.jar /


disable 'stats'
drop 'stats'
create 'stats', 'data'
alter 'stats', 'Coprocessor' => '/11.jar|coprocessor.ObserverStatisticsEndpoint|'


disable 'testtable'
drop 'testtable'
create 'testtable', 'colfam1'
alter 'testtable', 'Coprocessor' => '/4.jar|coprocessor.ObserverStatisticsEndpoint|'


disable 'my'
drop 'my'
create 'my', 'stats'
alter 'my', 'Coprocessor' => '/5.jar|coprocessor.ObserverStatisticsEndpoint|'

disable 'testtable'
drop 'testtable'
create 'testtable', 'colfam1'
alter 'testtable', 'Coprocessor' => '/5.jar|coprocessor.ObserverStatisticsEndpoint|'






create 'testtable', 'colfam1', 'colfam2'
put 'testtable', 'data1', 'colfam1:value', 'value1'

get 'test', 'data1'
   */

    private static final byte[] dataColF = Bytes.toBytes("data");

    private static long getValue(Result result, byte[] col) {
        return Bytes.toLong(result.getValue(dataColF, col));
    }

    public static void main(String[] args) throws IOException {

        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);
        HBaseHelper helper = HBaseHelper.getHelper(conf);

        try {

            final byte[] colf = Bytes.toBytes("data");


            TableName tableName = TableName.valueOf("stats");
            table = connection.getTable(tableName);

            byte[] row = Bytes.toBytes(1L);

            getAndPringRows(row);

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
        long avgVal = getValue(result, avg);

        System.out.println("countVal = " + countVal);
        System.out.println("minVal = " + minVal);
        System.out.println("maxVal = " + maxVal);
        System.out.println("avgVal = " + avgVal);
    }
}
