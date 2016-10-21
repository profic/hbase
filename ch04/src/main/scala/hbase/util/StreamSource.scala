package hbase.util

import java.util.{Calendar, Date}

import org.apache.commons.lang3.time.DateUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.annotation.switch
import scala.collection.mutable
import scala.concurrent.Future
import scala.util.Random

/**
  * Created by cloudera on 10/21/16.
  */
object StreamSource {

  def subscribeToStream(streamType: String, ssc: StreamingContext): DStream[(Long, Long)] = {

    (streamType: @switch) match {
      case "network" => networkStream(ssc)
      case _         => queueRandomStream(ssc)
    }
  }

  def networkStream(ssc: StreamingContext): DStream[(Long, Long)] = {
    ssc.socketTextStream("localhost", 9999, StorageLevel.MEMORY_AND_DISK_SER)
    .map(_.split(" ") match {
      case Array(timestamp, value) =>
        (timestamp.toLong, value.toLong)
    })
  }

  def queueRandomStream(ssc: StreamingContext): InputDStream[(Long, Long)] = {
    val queue = new mutable.SynchronizedQueue[RDD[(Long, Long)]]()

    val stream = ssc.queueStream(queue)

    Future {
      val date: Date = new Date()

      while (true) {
        val tuples = (1 to 300) map { e =>
          val truncated: Date = DateUtils.truncate(date, Calendar.MINUTE)
          val toBeSet: Date = DateUtils.addMinutes(truncated, -Random.nextInt(5))

          val timestamp: Long = toBeSet.getTime
          val value: Long = Random.nextInt(1000)
          (timestamp, value)
        }
        queue += ssc.sparkContext.parallelize(tuples)
        Thread.sleep(500)
      }
    }
    stream
  }

}
