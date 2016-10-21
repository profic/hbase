package hbase.domain

/**
  * Created by cloudera on 10/21/16.
  */
case class StatisticsEntity(timestamp: Long, count: Long, max: Long, min: Long, avg: Double)
