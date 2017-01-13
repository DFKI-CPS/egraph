package de.dfki.cps.egraph.internal

import org.neo4j.graphdb.{Direction, GraphDatabaseService, Node, Transaction}
import org.neo4j.graphdb.RelationshipType

import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}
import scala.collection.JavaConverters._

/**
  * @author Martin Ring <martin.ring@dfki.de>
  */
object Util {
  def deleteTransitiveOut(node: Node, types: RelationshipType*): Unit = {
    node.getRelationships(Direction.INCOMING).asScala.foreach { in =>
      in.delete()
    }
    node.getRelationships(Direction.OUTGOING,types :_*).asScala.foreach { out =>
      deleteTransitiveOut(out.getEndNode, types :_*)
    }
    node.getRelationships(Direction.OUTGOING).asScala.foreach { rel =>
      rel.delete()
    }
    node.delete()
  }

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
