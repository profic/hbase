package hbase.domain

import org.apache.commons.math3.stat.descriptive.SummaryStatistics

/**
  * Created by cloudera on 10/20/16.
  */
case class Stat(var updated: Boolean = true) {
  private val delegate = new SummaryStatistics()

  def +(value: Long) = delegate.addValue(value)

  def getAvg = delegate.getMean

  def getMax: Long = delegate.getMax.toLong

  def getMin: Long = delegate.getMin.toLong

  def getCount: Long = delegate.getN

  override def toString = s"avg: $getAvg, max:$getMax, min:$getMin, count:$getCount"

}
