package de.dfki.cps.egraph

import java.io.File

import org.neo4j.graphdb.factory.GraphDatabaseFactory
import org.neo4j.graphdb.{GraphDatabaseService, Result, Transaction}

import scala.collection.JavaConverters._
import scala.collection.convert.Wrappers.SeqWrapper
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

/**
 * @author Martin Ring <martin.ring@dfki.de>
 */
class Neo4j(store: File) {
  lazy val graphDb: GraphDatabaseService = {
    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = {
        graphDb.shutdown()
      }
    })
    new GraphDatabaseFactory().newEmbeddedDatabase(store)
  }

  def shutdown() = graphDb.shutdown()

  def transaction[T](f: Transaction => T): Try[T] = {
    val tx = graphDb.beginTx()
    try {
      val res = f(tx)
      tx.success()
      Success(res)
    } catch {
      case NonFatal(e) =>
        println("error in transaction")
        e.printStackTrace()
        tx.failure()
        Failure(e)
    } finally {
      tx.close()
    }
  }

  def query(query: String, params: Map[String,AnyRef] = Map.empty) = {
    try {
      val result = graphDb.execute(query, params.asJava)
      result.asScala.toStream.map(x => ResultRow(result,x.asScala.toMap))
    } catch {
      case NonFatal(e) =>
        println("error while executing: " + query)
        e.printStackTrace()
        sys.error("abort")
    }
  }

}

class Node(underlying: org.neo4j.graphdb.Node) {
  val props = underlying.getAllProperties.asScala.toMap
  val id = underlying.getId
}

case class ResultRow(result: Result, row: Map[String,AnyRef]) {
  def apply[T](column: String)(implicit convert: ResultConverter[T]): T = get[T](column).get
  def get[T](column: String)(implicit convert: ResultConverter[T]): Option[T] =
    row.get(column).flatMap(convert.apply)
}

object Row {
  def unapplySeq(row: ResultRow): Option[Seq[Any]] = Some(row.result.columns.asScala.map(row.row.get(_).get))
}

trait ResultConverter[T] {
  def apply(value: AnyRef): Option[T]
  def isDefinedAt(value: AnyRef): Boolean
}

object ResultConverter {
  def apply[T](f: PartialFunction[AnyRef,T]) = new ResultConverter[T] {
    def apply(value: AnyRef) = f.lift(value)
    def isDefinedAt(value: AnyRef) = f.isDefinedAt(value)
  }
}

object Neo4j {
  def apply(path: String = "./db") = new Neo4j(new File(path))

  implicit val BooleanResult = ResultConverter[Boolean]{
    case s: java.lang.Boolean => s
  }

  implicit val StringResult = ResultConverter[String]{
    case s: String => s
  }

  implicit val LongResult = ResultConverter[Long]{
    case l: java.lang.Long => l
    case i: java.lang.Integer => i.toLong
  }

  implicit val NodeResult = ResultConverter[Node]{
    case n: org.neo4j.graphdb.Node => new Node(n)
  }

  implicit val NodeResultDirect = ResultConverter[org.neo4j.graphdb.Node]{
    case n: org.neo4j.graphdb.Node => n
  }

  implicit def OptionResult[T](implicit convert: ResultConverter[T]) = ResultConverter[Option[T]] {
    case null => None
    case s if convert.isDefinedAt(s) => convert(s)
  }

  implicit def SeqResult[T](implicit convert: ResultConverter[T]) = ResultConverter[Seq[T]] {
    case s: SeqWrapper[AnyRef] if s.underlying.forall(convert.isDefinedAt) =>
      s.underlying.map(convert.apply).map(_.get)
    case s: Seq[AnyRef] if s.forall(convert.isDefinedAt) =>
      s.map(convert.apply).map(_.get)
  }
}

trait CypherQuery { self =>
  val query: String
  val params: Map[String,AnyRef] = Map.empty
  def on(ps: (String,AnyRef)*) = new CypherQuery {
    override val query: String = self.query
    override val params: Map[String,AnyRef] = self.params ++ ps
  }
  def apply[T](f: ResultRow => T)(implicit neo4j: Neo4j, tx: Transaction): Seq[T] = {
    val t0 = System.currentTimeMillis()
    val res = neo4j.query(query, params).map(f).force
    val t1 = System.currentTimeMillis()
    val d = t1 - t0
    if (d > 200) println(s"[LONG RUNNING QUERY: $d ms] $query")
    res
  }
  def apply()(implicit neo4j: Neo4j, tx: Transaction): Stream[ResultRow] =
    neo4j.query(query,params)
  def execute()(implicit neo4j: Neo4j, tx: Transaction): Boolean =
    { neo4j.query(query,params); true }
}

object Cypher {
  def apply(q: String) = new CypherQuery {
    override val query: String = q
  }
}