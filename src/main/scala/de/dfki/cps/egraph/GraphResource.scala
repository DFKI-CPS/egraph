package de.dfki.cps.egraph

import java.io.{File, InputStream, OutputStream}
import java.util
import java.util.concurrent.Executor

import de.dfki.cps.egraph.internal.NodeList
import org.eclipse.emf.common.util.{EList, URI}
import org.eclipse.emf.ecore.{EClass, EFactory, EObject, EOperation, EReference, EStructuralFeature, EcoreFactory}
import org.eclipse.emf.ecore.resource.impl.{ResourceImpl, ResourceSetImpl}

import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration
import internal.Util._
import org.eclipse.emf.common.notify.Notification
import org.eclipse.emf.ecore.util.EcoreUtil
import org.neo4j.graphdb.{Direction, Node}

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, Promise}

/**
  * @author Martin Ring <martin.ring@dfki.de>
  */
class GraphResource(uri: URI) extends ResourceImpl(uri) {
  implicit private val executionContext = ExecutionContext.fromExecutor(new Executor {
    def execute(command: Runnable) = command.run()
  })

  implicit val defaultTimeout = Duration.Inf

  val nodeMap = mutable.Map.empty[EObject,Node]

  val uriRequests = mutable.Map.empty[URI,EObject => Any]

  private def readValueNode(node: Node): Future[AnyRef] = {
    if (node.hasLabel(Labels.EObject)) Future.successful(readEObjectNode(node))
    else if (node.hasLabel(Labels.Reference)) {
      val uri = URI.createURI(node.getProperty("uri").asInstanceOf[String])
      val p = Promise[EObject]
      uriRequests += uri -> p.success
      p.future
    }
    else Future.successful(node.getProperty("value"))
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
          val vals = Future.sequence(values.map(readValueNode)).map(_.asJava)
          vals.onSuccess {
            case vals =>
              x.eGet(feature).asInstanceOf[EList[AnyRef]].addAll(vals)
          }
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
        uriRequests.foreach {
          case (uri,f) =>
            val x = resourceSet.getEObject(uri,true)
            if (x == null) sys.error("unresolvable uri " + uri)
            f(x)
        }
      }.get
    } else super.doLoad(inputStream,options)
  }

  private def createValueNode(store: EGraphStore, parent: EObject)(obj: AnyRef): Node = obj match {
    case ref: EObject if ref.eContainer() != parent =>
      val node = store.graphDb.createNode(Labels.Reference)
      node.setProperty("uri",EcoreUtil.getURI(ref).toString)
      node
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
        list.appendAll(values.map(createValueNode(store,obj)))
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
          case x: org.eclipse.emf.common.util.Enumerator =>
            x.getValue
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

/**object GraphResourceTest extends App {
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
**/