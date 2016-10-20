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

    private static void printStatistics(boolean print, boolean clear)
            throws Throwable {
        final StatisticsRequest request = StatisticsRequest
                .newBuilder().setClear(clear).build();
        Map<byte[], Map<String, Integer>> results = table.coprocessorService(
                ObserverStatisticsService.class,
                null, null,
                new Batch.Call<ObserverStatisticsProtos.ObserverStatisticsService,
                        Map<String, Integer>>() {
                    public Map<String, Integer> call(
                            ObserverStatisticsService statistics)
                            throws IOException {
                        BlockingRpcCallback<StatisticsResponse> rpcCallback =
                                new BlockingRpcCallback<StatisticsResponse>();
                        statistics.getStatistics(null, request, rpcCallback);
                        StatisticsResponse response = rpcCallback.get();
                        Map<String, Integer> stats = new LinkedHashMap<String, Integer>();
                        for (NameInt32Pair pair : response.getAttributeList()) {
                            stats.put(pair.getName(), pair.getValue());
                        }
                        return stats;
                    }
                }
        );
        if (print) {
            for (Map.Entry<byte[], Map<String, Integer>> entry : results.entrySet()) {
                System.out.println("Region: " + Bytes.toString(entry.getKey()));
                for (Map.Entry<String, Integer> call : entry.getValue().entrySet()) {
                    System.out.println("  " + call.getKey() + ": " + call.getValue());
                }
            }
            System.out.println();
        }
    }

  /*
  mvn clean install

mvn clean
mvn -Djar.finalName=10 install

sudo -u hdfs hadoop fs -rm /10.jar
sudo -u hdfs hadoop fs -put /home/cloudera/Downloads/hbase-book-master/ch04/target/10.jar /


disable 'stats'
drop 'stats'
create 'stats', 'data'
alter 'stats', 'Coprocessor' => '/10.jar|coprocessor.ObserverStatisticsEndpoint|'


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

            final byte[] count = Bytes.toBytes("count");
            final byte[] avg = Bytes.toBytes("avg");
            final byte[] min = Bytes.toBytes("min");
            final byte[] max = Bytes.toBytes("max");

            TableName tableName = TableName.valueOf("stats");
            table = connection.getTable(tableName);

            byte[] row = Bytes.toBytes(1L);

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

            System.out.println();

//            int countVal = Bytes.toInt(result.getValue(colf, count));
//            int minVal = Bytes.toInt(result.getValue(colf, min));
//            int maxVal = Bytes.toInt(result.getValue(colf, max));
//            int avgVal = Bytes.toInt(result.getValue(colf, avg));
//
//
//
//            Iterator<Result> it = table.getScanner(colf).iterator();
//            List<Result> list = new ArrayList<>();
//            while (it.hasNext()) {
//                list.add(it.next());
//            }
//
//            System.out.println(Bytes.toLong(CellUtil.cloneValue(list.get(0).getColumnCells(colf, count).get(0))));


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



            printStatistics(true, true);
//            stats(connection, cf);

        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

    }

    private static void stats(Connection connection, byte[] cf) throws Throwable {
        TableName tableName = TableName.valueOf("my");
        table = connection.getTable(tableName);

        Put put = new Put(Bytes.toBytes("1"));

        String newValueAttr = "newValue";
        put.addColumn(cf, Bytes.toBytes(newValueAttr),
                Bytes.toBytes("10"));
        put.setAttribute(newValueAttr, Bytes.toBytes(10L));
        table.put(put);

        Put put2 = new Put(Bytes.toBytes("1"));
        put2.addColumn(cf, Bytes.toBytes(newValueAttr),
                Bytes.toBytes("20"));
        put2.setAttribute(newValueAttr, Bytes.toBytes(20L));
        table.put(put2);

        Put put3 = new Put(Bytes.toBytes("1"));
        put3.addColumn(cf, Bytes.toBytes(newValueAttr),
                Bytes.toBytes("5"));
        put3.setAttribute(newValueAttr, Bytes.toBytes(5L));
        table.put(put3);

        printStatistics(true, true);

        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        for (Result result2 : scanner) {
            while (result2.advance())
                System.out.println("Cell: " + result2.current());
        }
    }

    private static void initialest(Connection connection) {
        try {
            TableName tableName = TableName.valueOf("testtable");
            table = connection.getTable(tableName);

            Put put = new Put(Bytes.toBytes("1"));
            put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("time1"),
                    Bytes.toBytes("value1"));
            table.put(put);

            Put put2 = new Put(Bytes.toBytes("1"));
            put2.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("time1"),
                    Bytes.toBytes("value2"));
            table.put(put2);

            printStatistics(true, true);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
    }
}
