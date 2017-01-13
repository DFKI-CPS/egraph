package de.dfki.cps.egraph

import java.io.{File, InputStream, OutputStream}
import java.util
import java.util.concurrent.Executor

import de.dfki.cps.egraph.internal.NodeList
import org.eclipse.emf.common.util.{EList, Enumerator, URI}
import org.eclipse.emf.ecore.{EAttribute, EClass, EFactory, EObject, EOperation, EReference, EStructuralFeature, EcoreFactory}
import org.eclipse.emf.ecore.resource.impl.{ResourceImpl, ResourceSetImpl}

import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration
import internal.Util._
import org.eclipse.emf.common.notify.Notification
import org.eclipse.emf.ecore.util.{EObjectContainmentEList, EcoreUtil}
import org.eclipse.emf.ecore.xmi.impl.XMLResourceImpl
import org.neo4j.graphdb.{Direction, Node}

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, Promise}

/**
  * @author Martin Ring <martin.ring@dfki.de>
  */
class GraphResource(uri: URI) extends ResourceImpl(uri) {
  implicit val defaultTimeout = Duration.Inf

  private val nodeMap = mutable.Map.empty[EObject,Node]
  private val objectMap = mutable.Map.empty[Node,EObject]

  private def readValueNode(node: Node): AnyRef = {
    Option(node.getSingleRelationship(Relations.EReferenceLink,Direction.OUTGOING)).fold {
      if (node.hasLabel(Labels.EObject)) readEObjectNode(node)
      else if (node.hasLabel(Labels.EReference)) {
        val uri = URI.createURI(node.getProperty("uri").asInstanceOf[String])
        resourceSet.getEObject(uri,true)
      }
      else node.getProperty("value")
    } { linked =>
      readEObjectNode(linked.getEndNode)
    }
  }

  private def readEObjectNode(node: Node): EObject = objectMap.get(node).getOrElse {
    val props = node.getAllProperties.asScala
    val refs = node.getRelationships(Direction.OUTGOING,Relations.EReference).asScala.map { rel =>
      rel.getProperty("name").asInstanceOf[String] -> rel.getEndNode
    }.toMap
    val eClass = resourceSet.getEObject(URI.createURI(props("eClass").asInstanceOf[String]),true).asInstanceOf[EClass]
    val factory = eClass.getEPackage.getEFactoryInstance
    val x = factory.create(eClass)
    nodeMap += x -> node
    objectMap += node -> x
    eClass.getEAllStructuralFeatures.asScala
      .filter(f => !f.isTransient && !f.isDerived).foreach {
        case ref: EReference =>
          refs.get(ref.getName).foreach { node =>
            if (ref.isMany) {
              val list = new NodeList(node)
              val values = list.map(readValueNode)
              val elist = x.eGet(ref).asInstanceOf[EList[AnyRef]]
              elist.addAll(values.asJava)
            } else {
              x.eSet(ref,readValueNode(node))
            }
          }
        case attr: EAttribute =>
          props.get(attr.getName).foreach { value =>
            val attrType = attr.getEAttributeType
            val factory = attrType.getEPackage.getEFactoryInstance
            if (attr.isMany)
              x.eGet(attr).asInstanceOf[EList[AnyRef]].addAll(
                value.asInstanceOf[Array[String]].map(factory.createFromString(attrType,_)).toSeq.asJava
              )
            else {
              factory.createFromString(attrType,value.asInstanceOf[String])
            }
          }
    }
    x
  }

  override def doLoad(inputStream: InputStream, options: util.Map[_, _]): Unit = {
    if (uri.scheme() == "graph" && inputStream.isInstanceOf[EGraphStoreInput]) {
      val store = inputStream.asInstanceOf[EGraphStoreInput].store
      this.nodeMap.clear()
      this.objectMap.clear()
      store.graphDb.transaction {
        val resource = Option(store.graphDb.findNode(Labels.Resource,"uri",uri.host()))
        this.contents.clear()
        resource.foreach { root =>
          val contents = new NodeList(root.getSingleRelationship(Relations.Contents,Direction.OUTGOING).getEndNode)
          this.contents.addAll(contents.map(readEObjectNode).asJava)
        }
      }.get
    } else super.doLoad(inputStream,options)
  }

  private def createValueNode(store: EGraphStore, containment: Boolean)(obj: AnyRef): Node = obj match {
    case eObj: EObject if containment => createEObjectNode(store)(eObj)
    case ref: EObject =>
      val uri = EcoreUtil.getURI(ref)
      val node = store.graphDb.createNode(Labels.EReference)
      node.setProperty("uri",uri.toString)
      if (uri.scheme() == "graph" && uri.host == this.uri.host()) {
        val refNode = createEObjectNode(store)(ref)
        node.createRelationshipTo(refNode,Relations.EReferenceLink)
      }
      node
    case other: AnyRef =>
      val node = store.graphDb.createNode(Labels.Literal)
      node.setProperty("value",obj)
      node
  }

  private def createEObjectNode(store: EGraphStore)(obj: EObject): Node = nodeMap.get(obj).getOrElse {
    val node = store.graphDb.createNode(Labels.EObject)
    nodeMap += obj -> node
    objectMap += node -> obj
    node.setProperty("eClass", EcoreUtil.getURI(obj.eClass()).toString)
    obj.eClass().getEAllStructuralFeatures.asScala
      .filter(f => !f.isTransient && !f.isDerived).filter(obj.eIsSet).foreach {
        case ref: EReference =>
          if (ref.isMany) {
            val listNode = store.graphDb.createNode(Labels.EList)
            val list = new NodeList(listNode)
            val rel = node.createRelationshipTo(listNode,Relations.EReference)
            rel.setProperty("name",ref.getName)
            val values = obj.eGet(ref).asInstanceOf[EList[AnyRef]].asScala
            list.appendAll(values.map(createValueNode(store,ref.isContainment)))
          } else {
            val feature = createValueNode(store,ref.isContainment)(obj.eGet(ref))
            val rel = node.createRelationshipTo(feature,Relations.EReference)
            rel.setProperty("name",ref.getName)
          }
        case attr: EAttribute =>
          val attrType = attr.getEAttributeType
          val factory = attrType.getEPackage.getEFactoryInstance
          if (attr.isMany) {
            val values = obj.eGet(attr).asInstanceOf[EList[AnyRef]].asScala
              .map(factory.convertToString(attrType,_))
            node.setProperty(attr.getName, values.toArray)
          } else {
            val value = factory.convertToString(attrType,obj.eGet(attr))
            node.setProperty(attr.getName, value)
          }
      }
    node
  }

  override def doSave(outputStream: OutputStream, options: util.Map[_, _]): Unit = {
    if (uri.scheme() == "graph" && outputStream.isInstanceOf[EGraphStoreOutput]) {
      val store = outputStream.asInstanceOf[EGraphStoreOutput].store
      store.graphDb.transaction {
        // delete old resource if existes
        Option(store.graphDb.findNode(Labels.Resource,"uri",uri.host()))
          .foreach(deleteTransitiveOut(_,Relations.Contents,Relations.NextSibling,Relations.EReference))
        // create new resource
        val resource = store.graphDb.createNode(Labels.Resource)
        resource.setProperty("uri",uri.host())
        // write contents
        val contents = store.graphDb.createNode(Labels.EList)
        val list = new NodeList(contents)
        list.appendAll(this.contents.asScala.map(createEObjectNode(store)))
        resource.createRelationshipTo(contents,Relations.Contents)
      }.get
    } else super.doSave(outputStream,options)
 }
}