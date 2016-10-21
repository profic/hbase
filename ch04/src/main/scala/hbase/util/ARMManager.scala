package hbase.util

import java.io.Closeable

/**
  * Created by cloudera on 10/21/16.
  */
object ARMManager {

  def cleanly[A <: Closeable, B](resource: => A)(code: A => B): Either[Exception, B] = {
    try {
      val r = resource
      try {
        Right(code(r))
      } finally {
        r.close()
      }
    }
    catch {
      case e: Exception => Left(e)
    }
  }

}
