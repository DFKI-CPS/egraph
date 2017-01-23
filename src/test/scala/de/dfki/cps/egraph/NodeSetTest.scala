package de.dfki.cps.egraph

import org.neo4j.graphdb.{GraphDatabaseService, Node}
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import internal.Util._

import scala.collection.mutable

/**
  * @author Martin Ring <martin.ring@dfki.de>
  */
class NodeSetTest extends FlatSpec with Matchers with BeforeAndAfterAll {
  val dir = new java.io.File("testDB")
  if (dir.exists()) dir.delete()
  dir.mkdirs()

  lazy val graphDb: GraphDatabaseService = {
    val builder = new GraphDatabaseFactory()
      .newEmbeddedDatabaseBuilder(dir)
    val db = builder.newGraphDatabase()
    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = db.shutdown()
    })
    db
  }

  override def beforeAll(): Unit = {
    graphDb
  }

  val n = 100000

  "a nodeset" should s"be able to hold $n nodes" in {
    graphDb.transaction {
      val nodes = for (i <- 0 to n) yield {
        val node = graphDb.createNode()
        node.setProperty("value",i)
        node
      }
      val set = internal.NodeSet.empty(graphDb,unique = true)
      set ++= nodes
      set.size shouldBe nodes.length
      set.map(_.getProperty("value").asInstanceOf[Int]) shouldBe (0 to n).toSet
    }.get
  }

  it should "allow removal of elements" in {
    graphDb.transaction {
      val nodes = for (i <- 1 to 10) yield {
        val node = graphDb.createNode()
        node.setProperty("value",i)
        node
      }
      val set = internal.NodeSet.empty(graphDb,unique = true)
      set ++= nodes
      val sset = mutable.Set(nodes :_*)
      set shouldBe sset
      set --= nodes.drop(2).take(4)
      sset --= nodes.drop(2).take(4)
      set shouldBe sset
      set.clear()
      set shouldBe empty
    }.get
  }


  it should "behave the same as mutable.Set for complex operations" in {
    graphDb.transaction {
      val nodes = for (i <- 1 to 20) yield {
        val node = graphDb.createNode()
        node.setProperty("value",i)
        node
      }
      val set = internal.NodeSet.empty(graphDb,unique = true)
      set ++= nodes
      val sset = mutable.Set(nodes :_*)
      def doStuff(set: mutable.Set[Node]) = {
        val values = set.toList.sortBy(_.getProperty("value").asInstanceOf[Int])
        set.clear()
        set ++= values.reverse
        set += values(5)
        set += values(5)
        set += values(5)
        set += values(5)
        set.remove(values(2))
        set -= values(5)
        set -= values(5)
        set.remove(values(7))
        val remove = values.collect {
          case node if node.getProperty("value").asInstanceOf[Int] % 2 == 0 => node
        }
        set --= remove
        set ++= remove
        set.map(_.getProperty("value"))
      }
      doStuff(set) shouldBe doStuff(sset)
    }.get
  }

  "a unique nodeset" should "never hold the same node twice" in {
    graphDb.transaction {
      val nodes = for (i <- 1 to 20) yield {
        val node = graphDb.createNode()
        node.setProperty("value",i)
        node
      }
      val set = internal.NodeSet.empty(graphDb,unique = true)
      set ++= nodes
      set ++= nodes
      set
      set.size should be (nodes.size)

    }.get
  }

  "a non-unique nodeset" should "be able to hold the same node twice" in {
    graphDb.transaction {
      val nodes = for (i <- 1 to 20) yield {
        val node = graphDb.createNode()
        node.setProperty("value",i)
        node
      }
      val set = internal.NodeSet.empty(graphDb,unique = false)
      set ++= nodes
      set ++= nodes
      set.size should be (nodes.size * 2)
      set.remove(nodes(3))
      set.size should be (nodes.size * 2 - 1)
    }.get
  }

  override def afterAll(): Unit = graphDb.shutdown()
}
