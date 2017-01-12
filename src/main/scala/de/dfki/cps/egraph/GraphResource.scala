package de.dfki.cps.egraph

import java.io.{File, InputStream, OutputStream}
import java.util

import org.eclipse.emf.common.util.URI
import org.eclipse.emf.ecore.EcoreFactory
import org.eclipse.emf.ecore.resource.impl.{ResourceImpl, ResourceSetImpl}

import scala.concurrent.duration.Duration
import internal.Util._

/**
  * @author Martin Ring <martin.ring@dfki.de>
  */
class GraphResource(uri: URI) extends ResourceImpl(uri) {
  implicit val defaultTimeout = Duration.Inf

  override def doLoad(inputStream: InputStream, options: util.Map[_, _]): Unit = {
    if (this.getURI.scheme() == "graph" && inputStream.isInstanceOf[EGraphStoreInput]) {
      val store = inputStream.asInstanceOf[EGraphStoreInput].store
      store.graphDb.transaction {
        val resource = Option(store.graphDb.findNode(Labels.Resource,"uri",getURI.host()))
        resource.fold(this.contents.clear()) { root =>

        }
      }
    } else super.doLoad(inputStream,options)
  }

  override def doSave(outputStream: OutputStream, options: util.Map[_, _]): Unit = {
    if (this.getURI.scheme() == "graph" && outputStream.isInstanceOf[EGraphStoreOutput]) {
      val store = outputStream.asInstanceOf[EGraphStoreOutput].store
      store.graphDb.transaction {
        val resource = Option {
          store.graphDb.findNode(Labels.Resource,"uri",getURI.host())
        } getOrElse {
          store.graphDb.createNode(Labels.Resource)
        }
        resource.setProperty("uri",getURI.host())
      }
    } else super.doSave(outputStream,options)
 }
}

object GraphResourceTest extends App {
  val rs = new ResourceSetImpl
  val db = new File("db")
  val factory = EcoreFactory.eINSTANCE
  if (!db.exists()) db.mkdirs()
  val store = new EGraphStore(new File("db"))
  store.attach(rs)
  val res = rs.createResource(URI.createURI("graph://bla"))
  //res.load(new util.HashMap)
  val pkg = factory.createEPackage()
  pkg.setName("test")
  res.getContents.add(pkg)
  res.save(new util.HashMap)
  import scala.collection.JavaConverters._
  store.graphDb.transaction {
    store.graphDb.getAllNodes.asScala.foreach { node =>
      println(node.getId + node.getLabels.asScala.mkString(":",";",""))
      node.getAllProperties.asScala.map("  " + _)foreach(println)
    }
  }
}
