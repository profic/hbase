package coprocessor;

import hbase.dao.StatisticCoprocessorAwareEventHbaseDao;
import hbase.domain.Event;
import hbase.service.EventProcessor;
import hbase.service.EventProcessorImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.util.Date;

import static org.junit.Assert.assertEquals;


/**
 * Created by cloudera on 10/20/16.
 */
public class IntegrationTest extends AbstractTest {

    @Test
    public void baseTest() throws Exception {
        StatisticCoprocessorAwareEventHbaseDao dao =
                new StatisticCoprocessorAwareEventHbaseDao(HBaseConfiguration.create());

        EventProcessor eventProcessor = new EventProcessorImpl(dao);

        long rowKey = new Date().getTime();

        eventProcessor.accept(new Event(rowKey, 2));
        eventProcessor.accept(new Event(rowKey, 4));
        eventProcessor.accept(new Event(rowKey, 9));

        byte[] row = Bytes.toBytes(rowKey);
        Get get = new Get(row);
        get.addColumn(dataColF, count);
        get.addColumn(dataColF, min);
        get.addColumn(dataColF, max);
        get.addColumn(dataColF, avg);

        Configuration conf = HBaseConfiguration.create();

        try (Connection connection = ConnectionFactory.createConnection(conf);
             Table table = connection.getTable(tableName)) {
            Result result = table.getScanner(dataColF).next();

            long countVal = getValue(result, count);
            double avgVal = Bytes.toDouble(result.getValue(dataColF, avg));
            long maxVal = getValue(result, max);
            long minVal = getValue(result, min);

            assertEquals(3, countVal);
            assertEquals(9, maxVal);
            assertEquals(2, minVal);
            assertEquals(5, avgVal, 0.1);
        }


    }

}
