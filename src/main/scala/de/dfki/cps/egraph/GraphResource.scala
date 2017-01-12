package de.dfki.cps.egraph

import java.io.{File, InputStream, OutputStream}
import java.util

import de.dfki.cps.egraph.internal.NodeList
import org.eclipse.emf.common.util.{EList, URI}
import org.eclipse.emf.ecore.{EClass, EFactory, EObject, EcoreFactory}
import org.eclipse.emf.ecore.resource.impl.{ResourceImpl, ResourceSetImpl}

import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration
import internal.Util._
import org.eclipse.emf.ecore.util.EcoreUtil
import org.neo4j.graphdb.{Direction, Node}

import scala.collection.mutable

/**
  * @author Martin Ring <martin.ring@dfki.de>
  */
class GraphResource(uri: URI) extends ResourceImpl(uri) {
  implicit val defaultTimeout = Duration.Inf

  val nodeMap = mutable.Map.empty[EObject,Node]

  private def readValueNode(node: Node): AnyRef = {
    if (node.hasLabel(Labels.EObject)) readEObjectNode(node)
    else node.getProperty("value")
  }

  private def readEObjectNode(node: Node): EObject = {
    val props = node.getAllProperties.asScala
    val manyProps = node.getRelationships(Direction.OUTGOING,Relations.Feature).asScala.map { rel =>
      rel.getProperty("name").asInstanceOf[String] -> new NodeList(rel.getEndNode)
    }.toMap
    val eClass = resourceSet.getEObject(URI.createURI(props("eClass").asInstanceOf[String]),true).asInstanceOf[EClass]
    val factory = eClass.getEPackage.getEFactoryInstance
    val x = factory.create(eClass)
    nodeMap += x -> node
    eClass.getEAllStructuralFeatures.asScala
      .filter(f => !f.isTransient && !f.isDerived).foreach { feature =>
      if (feature.isMany) {
        manyProps.get(feature.getName).foreach { values =>
          x.eGet(feature).asInstanceOf[EList[AnyRef]].addAll(values.map(readValueNode).asJava)
        }
      } else props.get(feature.getName).foreach { value =>
        x.eSet(feature,value)
      }
    }
    x
  }

  override def doLoad(inputStream: InputStream, options: util.Map[_, _]): Unit = {
    if (this.getURI.scheme() == "graph" && inputStream.isInstanceOf[EGraphStoreInput]) {
      val store = inputStream.asInstanceOf[EGraphStoreInput].store
      this.nodeMap.clear()
      store.graphDb.transaction {
        val resource = Option(store.graphDb.findNode(Labels.Resource,"uri",getURI.host()))
        this.contents.clear()
        resource.foreach { root =>
          val contents = new NodeList(root.getSingleRelationship(Relations.Contents,Direction.OUTGOING).getEndNode)
          this.contents.addAll(contents.map(readEObjectNode).asJava)
        }
      }.get
    } else super.doLoad(inputStream,options)
  }

  private def createValueNode(store: EGraphStore)(obj: AnyRef): Node = obj match {
    case proxy: EObject if proxy.eIsProxy() => ???
    case eObj: EObject => createEObjectNode(store)(eObj)
    case other: AnyRef =>
      val node = store.graphDb.createNode(Labels.Literal)
      node.setProperty("value",obj)
      node
  }

  private def createEObjectNode(store: EGraphStore)(obj: EObject): Node = {
    val node = store.graphDb.createNode(Labels.EObject)
    nodeMap += obj -> node
    node.setProperty("eClass", EcoreUtil.getURI(obj.eClass()).toString)
    obj.eClass().getEAllStructuralFeatures.asScala
      .filter(f => !f.isTransient && !f.isDerived).filter(obj.eIsSet).foreach { feature =>
      if (feature.isMany) {
        val listNode = store.graphDb.createNode(Labels.EList)
        val list = new NodeList(listNode)
        val rel = node.createRelationshipTo(listNode,Relations.Feature)
        rel.setProperty("name",feature.getName)
        val values = obj.eGet(feature).asInstanceOf[EList[AnyRef]].asScala
        list.appendAll(values.map(createValueNode(store)))
      } else {
        val value = obj.eGet(feature) match {
          case i: java.lang.Integer => i
          case s: java.lang.String => s
          case l: java.lang.Long => l
          case b: java.lang.Boolean => b
          case f: java.lang.Float => f
          case d: java.lang.Double => d
          case s: java.lang.Short => s
          case b: java.lang.Byte => b
          case c: java.lang.Character => c
          case e: EFactory => Option(e.getEPackage.getNsURI).getOrElse("<null>")
          case e: EObject => EcoreUtil.getURI(e).toString
        }
        node.setProperty(feature.getName, value)
      }
    }
    node
  }

  override def doSave(outputStream: OutputStream, options: util.Map[_, _]): Unit = {
    if (this.getURI.scheme() == "graph" && outputStream.isInstanceOf[EGraphStoreOutput]) {
      val store = outputStream.asInstanceOf[EGraphStoreOutput].store
      store.graphDb.transaction {
        // delete old resource if existes
        Option(store.graphDb.findNode(Labels.Resource,"uri",getURI.host())).foreach(deleteTransitiveOut)
        // create new resource
        val resource = store.graphDb.createNode(Labels.Resource)
        resource.setProperty("uri",getURI.host())
        // write contents
        val contents = store.graphDb.createNode(Labels.EList)
        val list = new NodeList(contents)
        list.appendAll(this.contents.asScala.map(createEObjectNode(store)))
        resource.createRelationshipTo(contents,Relations.Contents)
      }.get
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
  val cls = factory.createEClass()
  cls.setName("Blub")
  cls.setAbstract(true)
  pkg.getEClassifiers.add(cls)
  pkg.setName("test")
  res.getContents.add(pkg)
  res.save(new util.HashMap)
  import scala.collection.JavaConverters._
  store.graphDb.transaction {
    //res.delete(Map.empty.asJava)
    store.graphDb.getAllNodes.asScala.foreach { node =>
      println(node.getId + node.getLabels.asScala.mkString(":",";",""))
      node.getAllProperties.asScala.map("  " + _)foreach(println)
      node.getRelationships(Direction.OUTGOING).asScala.foreach { rel =>
        println("    -> " + rel.getEndNode.getId + ":" + rel.getType + rel.getAllProperties.asScala.mkString(":",";",""))
      }
    }
  }
  res.unload()
  res.load(new util.HashMap)
  def print(level: Int)(obj: EObject): Unit = {
    println(List.fill(level * 2)(" ").mkString + obj)
    obj.eContents().asScala.foreach(print(level + 1))
  }
  res.getContents.asScala.foreach(print(0))
}
