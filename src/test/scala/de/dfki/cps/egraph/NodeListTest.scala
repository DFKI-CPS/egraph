package de.dfki.cps.egraph

import org.neo4j.graphdb.{GraphDatabaseService, Node}
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import internal.Util._

import scala.collection.mutable

/**
  * @author Martin Ring <martin.ring@dfki.de>
  */
class NodeListTest extends FlatSpec with Matchers with BeforeAndAfterAll {
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

  "a nodelist" should "be able to hold 100000 nodes" in {
    graphDb.transaction {
      val nodes = for (i <- 0 to 100000) yield {
        val node = graphDb.createNode()
        node.setProperty("value",i)
        node
      }
      val list = internal.NodeList.from(nodes,graphDb)
      list.length shouldBe nodes.length
      list(1000).getProperty("value") shouldBe 1000
      list(99000).getProperty("value") shouldBe 99000
      list.map(_.getProperty("value").asInstanceOf[Int]) shouldBe (0 to 100000)

      val list2 = new internal.NodeList(list.root)

      list shouldBe list2
    }.get
  }

  it should "allow removal of elements" in {
    graphDb.transaction {
      val nodes = for (i <- 1 to 10) yield {
        val node = graphDb.createNode()
        node.setProperty("value",i)
        node
      }
      val list = internal.NodeList.from(nodes,graphDb)
      val list2 = new internal.NodeList(list.root)
      val slist = mutable.Buffer(nodes :_*)
      list shouldBe slist
      list shouldBe list2
      list.size shouldBe slist.size
      list.size shouldBe list2.size
      list.remove(3,2)
      slist.remove(3,2)
      list shouldBe slist
      list shouldBe list2
      list.size shouldBe slist.size
      list.size shouldBe list2.size
      list.clear()
      slist.clear()
      list shouldBe empty
      list shouldBe list2
      list.size shouldBe slist.size
      list.size shouldBe list2.size
    }.get
  }

  it should "behave the same as mutable.Buffer for complex operations" in {
    graphDb.transaction {
      val nodes = for (i <- 1 to 20) yield {
        val node = graphDb.createNode()
        node.setProperty("value",i)
        node
      }
      val list = internal.NodeList.from(nodes,graphDb)
      val slist = mutable.Buffer(nodes :_*)
      def doStuff(buf: mutable.Buffer[Node]) = {
        val values = buf.toList
        buf.clear()
        buf.appendAll(values.reverse)
        buf.remove(2)
        buf.remove(5)
        buf.remove(7)
        val remove = buf.reverse.collect {
          case node if node.getProperty("value").asInstanceOf[Int] % 2 == 0 => node
        }
        buf --= remove
        buf.prependAll(remove.reverse)
        buf.trimStart(2)
        buf.trimEnd(2)
        buf.map(_.getProperty("value"))
      }
      doStuff(list) shouldBe doStuff(slist)
    }.get
  }

  override def afterAll(): Unit = graphDb.shutdown()
}
