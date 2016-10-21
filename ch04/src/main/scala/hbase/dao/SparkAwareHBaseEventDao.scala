package hbase.dao

import java.util.{Calendar, Date}

import hbase.domain.StatisticsEntity
import org.apache.commons.lang.time.DateUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes

/**
  * Created by cloudera on 10/21/16.
  */
class SparkAwareHBaseEventDao(private val conf: Configuration) {

  import hbase.util.ARMManager._

  private val tableName: TableName = TableName.valueOf("stats")
  private val dataColF: Array[Byte] = Bytes.toBytes("data")
  private val count: Array[Byte] = Bytes.toBytes("count")
  private val avg: Array[Byte] = Bytes.toBytes("avg")
  private val min: Array[Byte] = Bytes.toBytes("min")
  private val max: Array[Byte] = Bytes.toBytes("max")

  def save(event: StatisticsEntity): Either[Exception, Unit] = {
    cleanly(ConnectionFactory.createConnection(conf)) { connection =>
      cleanly(connection.getTable(tableName)) { table =>
        val time: Long = DateUtils.truncate(new Date(event.timestamp), Calendar.MINUTE).getTime
        val row: Array[Byte] = Bytes.toBytes(time)
        val put: Put = new Put(row)
        put.addColumn(dataColF, count, Bytes.toBytes(event.count))
        put.addColumn(dataColF, avg, Bytes.toBytes(event.avg))
        put.addColumn(dataColF, max, Bytes.toBytes(event.max))
        put.addColumn(dataColF, min, Bytes.toBytes(event.min))
        table.put(put)
      }
    }
  }
}