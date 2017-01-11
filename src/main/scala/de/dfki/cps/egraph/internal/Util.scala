package de.dfki.cps.egraph.internal

import org.neo4j.graphdb.{GraphDatabaseService, Transaction}

import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

/**
  * @author Martin Ring <martin.ring@dfki.de>
  */
object Util {
  implicit class RichGraphDatabase(val graphDb: GraphDatabaseService) extends AnyVal {
    def transaction[T](body: => T)(implicit timeout: Duration = Duration.Inf): Try[T] =
      withTransaction(_ => body)

    def withTransaction[T](body: Transaction => T)(implicit timeout: Duration = Duration.Inf): Try[T] = {
      val tx = if (timeout == Duration.Inf)
        graphDb.beginTx()
      else graphDb.beginTx(timeout._1, timeout._2)
      val res = Try {
        val res = body(tx)
        tx.success()
        res
      }
      res.transform({ success =>
        tx.close()
        Success(success)
      },{ fail =>
        tx.failure()
        Try(tx.close()).failed.foreach(fail.addSuppressed)
        Failure(fail)
      })
    }
  }
}
