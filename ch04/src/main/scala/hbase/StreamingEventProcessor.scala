package hbase

import hbase.dao.HBaseStatisticsDao
import hbase.domain.{Stat, StatisticsEntity}
import hbase.util.{LogHelper, StreamSource}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{Logging, SparkConf}

import scala.collection._

object StreamingEventProcessor extends Logging {

  LogHelper.setStreamingLogLevels()

  private val hbaseConfig: Configuration = HBaseConfiguration.create
  private val dao: HBaseStatisticsDao = new HBaseStatisticsDao(hbaseConfig)
  // change to yours <hdfs path>
  private val checkpointDir: String = "/"

  def main(args: Array[String]) {

    val streamType = if (args.length == 0) "queue" else args(0)

    // don't understand should it be 1 minute or optimal value to handle events?
    val emitInterval = Seconds(3)

    val sparkConf = new SparkConf().setAppName("SparkEventProcessor")
    val ssc = new StreamingContext(sparkConf, emitInterval)

    ssc.checkpoint(checkpointDir)

    val eventsStream = StreamSource.subscribeToStream(streamType, ssc)

    val results = eventsStream.updateStateByKey[Stat](updateStatistic)
                  .filter({ case (_, Stat(updated)) ⇒ updated })

    results foreachRDD { rdd ⇒
      rdd.map(toEntity).foreach(e ⇒ {
        dao.save(e) match {
          case Left(ex) ⇒
            logDebug(s"Exception during save $e", ex)
          case _        ⇒
        }
      })
    }

    results.print()

    ssc.start()
    ssc.awaitTermination()

  }

  def toEntity(t: (Long, Stat)): StatisticsEntity = t match {
    case (timestamp, stat) ⇒
      StatisticsEntity(
        timestamp,
        stat.getCount,
        stat.getMax,
        stat.getMin,
        stat.getAvg
      )
  }

  val updateStatistic = (count: Seq[Long], state: Option[Stat]) ⇒ {
    if (count.nonEmpty) {
      val current = state.getOrElse(Stat())
      current + count.sum
      current.updated = true
      Some(current)
    } else {
      state.flatMap(s ⇒ {
        s.updated = false
        Some(s)
      })
    }
  }
}