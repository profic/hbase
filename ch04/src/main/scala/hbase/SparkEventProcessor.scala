/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
package hbase

import java.util.{Calendar, Date}

import hbase.dao.SparkAwareHBaseEventDao
import hbase.domain.{Stat, StatisticsEntry}
import org.apache.commons.lang3.time.DateUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf}

import scala.Seq
import scala.annotation.switch
import scala.collection._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.Random

object SparkEventProcessor {

  LogHelper.setStreamingLogLevels()

  private val hbaseConfig: Configuration = HBaseConfiguration.create
  private val dao: SparkAwareHBaseEventDao = new SparkAwareHBaseEventDao(hbaseConfig)
  private val checkpointDir: String = "/user/cloudera/result"

  /*
  spark-submit --master local[4] --class hbase.SparkEventProcessor target/hbase-task.jar
   */

  def remain(timestamp: Long): Boolean = true

  val updateStatistic = (values: Seq[Long], state: Option[Stat]) => {
    if (values.nonEmpty) {
      val current = state.getOrElse(Stat())
      current + values.sum
      current.updated = true
      Some(current)
    } else {
      state.flatMap(s => {
        s.updated = false
        Some(s)
      })
    }
  }

  def remain(t: (Long, Seq[Long], Option[Stat])): Boolean = t._3.getOrElse(Stat()).getCount < 5

  val updateStatisticWithTimestamp = (it: Iterator[(Long, Seq[Long], Option[Stat])]) => {

    it.filter(t => remain(t))
    .map { case (timestamp, values, stat) =>
      (timestamp, updateStatistic(values, stat).getOrElse(Stat()))
    }
  }

  def main(args: Array[String]) {

    val streamType = if (args.length == 0) "queue" else args(0)

    // don't understand should it be 1 minute or optimal value to handle events?
    val emitInterval = Seconds(3)

    val sparkConf = new SparkConf().setAppName("SparkEventProcessor")
    val ssc = new StreamingContext(sparkConf, emitInterval)

    ssc.checkpoint(checkpointDir)

    val eventsStream = subscribeToStream(streamType, ssc)

    val results = eventsStream.updateStateByKey[Stat](updateStatisticWithTimestamp, new HashPartitioner(10), rememberPartitioner = true)
                  .filter({ case (_, Stat(updated)) => updated })

    results foreachRDD {
      _.map(toEntry).foreach(dao.save)
    }

    results.print()

    ssc.start()
    ssc.awaitTermination()
  }

  def toEntry(t: (Long, Stat)): StatisticsEntry = t match {
    case (timestamp, stat) =>
      val entry: StatisticsEntry = new StatisticsEntry

      entry.setTimestamp(timestamp)
      entry.setAvg(stat.getAvg)
      entry.setCount(stat.getCount)
      entry.setMax(stat.getMax)
      entry.setMin(stat.getMin)
      entry
  }

  def subscribeToStream(streamType: String, ssc: StreamingContext): DStream[(Long, Long)] = {

    (streamType: @switch) match {
      case "network" => networkStream(ssc)
      case _ => queueRandomStream(ssc)
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