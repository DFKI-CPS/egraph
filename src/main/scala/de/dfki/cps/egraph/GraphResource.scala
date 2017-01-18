package de.dfki.cps.egraph

import java.io.{InputStream, OutputStream}
import java.util

import de.dfki.cps.egraph.internal.{NodeList, NodeSet}
import de.dfki.cps.egraph.internal.Util._
import org.eclipse.emf.common.notify.Notification
import org.eclipse.emf.common.notify.impl.AdapterImpl
import org.eclipse.emf.common.util.{EList, URI}
import org.eclipse.emf.ecore.resource.impl.ResourceImpl
import org.eclipse.emf.ecore.util.EcoreUtil
import org.eclipse.emf.ecore.{EAttribute, EClass, EObject, EReference, EStructuralFeature, InternalEObject}
import org.neo4j.graphdb.{Direction, Node}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.duration.Duration

/**
  * @author Martin Ring <martin.ring@dfki.de>
  */
class GraphResource(uri: URI) extends ResourceImpl(uri) {
  implicit val defaultTimeout = Duration.Inf

  private val nodeMap = mutable.Map.empty[EObject,Node]
  private val objectMap = mutable.Map.empty[Node,EObject]

  def adapt(obj: EObject, node: Node) = new AdapterImpl {
    override def notifyChanged(msg: Notification): Unit = {
      super.notifyChanged(msg)
      val feature = Option(msg.getFeature).collect {
        case f: EStructuralFeature if !f.isTransient && !f.isDerived && !f.isVolatile => f
      }
      feature.foreach {
        case ref: EReference =>
          msg.getEventType match {
            case Notification.ADD =>
            case Notification.REMOVE =>
          }
        case attr: EAttribute =>
      }
    }
  }

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

  private def readEObjectNode(node: Node): EObject = objectMap.getOrElse(node, {
    val props = node.getAllProperties.asScala
    val refs = node.getRelationships(Direction.OUTGOING, Relations.EReference).asScala.map { rel =>
      rel.getProperty("name").asInstanceOf[String] -> rel.getEndNode
    }.toMap
    val eClass = resourceSet.getEObject(URI.createURI(props("eClass").asInstanceOf[String]), true).asInstanceOf[EClass]
    val factory = eClass.getEPackage.getEFactoryInstance
    val x = factory.create(eClass)
    nodeMap += x -> node
    objectMap += node -> x
    eClass.getEAllStructuralFeatures.asScala
      .filter(f => !f.isTransient && !f.isDerived).foreach {
      case ref: EReference =>
        refs.get(ref.getName).foreach { node =>
          if (ref.isMany) {
            val values = //if (ref.isOrdered) {
              new NodeList(node).map(readValueNode).asJavaCollection
            //} else {
              //new NodeSet(node, ref.isUnique).map(readValueNode).asJavaCollection
            //}
            val elist = x.eGet(ref).asInstanceOf[EList[AnyRef]]
            elist.addAll(values)
          } else {
            x.eSet(ref, readValueNode(node))
          }
        }
      case attr: EAttribute =>
        props.get(attr.getName).foreach { value =>
          val attrType = attr.getEAttributeType
          val factory = attrType.getEPackage.getEFactoryInstance
          if (attr.isMany)
            x.eGet(attr).asInstanceOf[EList[AnyRef]].addAll(
              value.asInstanceOf[Array[String]].map(factory.createFromString(attrType, _)).toSeq.asJava
            )
          else {
            x.eSet(attr,factory.createFromString(attrType, value.asInstanceOf[String]))
          }
        }
    }
    x.eAdapters.add(adapt(x,node))
    x
  })

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

  private def createValueNode(store: EGraphStore, containment: Boolean)(obj: EObject): Node = obj match {
    case eObj: EObject if containment => createEObjectNode(store)(eObj)
    case ref: EObject =>
      val uri = EcoreUtil.getURI(ref)
      val node = store.graphDb.createNode(Labels.EReference)
      if (uri.scheme() == "graph" && uri.authority() == this.uri.authority()) {
        val frag = EcoreUtil.getRelativeURIFragmentPath(EcoreUtil.getRootContainer(ref),ref)
        node.setProperty("uri",frag)
        val refNode = createEObjectNode(store)(ref)
        node.createRelationshipTo(refNode,Relations.EReferenceLink)
      } else {
        node.setProperty("uri",uri.toString)
      }
      node
  }

  private def createEObjectNode(store: EGraphStore)(obj: EObject): Node = nodeMap.getOrElse(obj, {
    val node = store.graphDb.createNode(Labels.EObject)
    nodeMap += obj -> node
    objectMap += node -> obj
    node.setProperty("eClass", EcoreUtil.getURI(obj.eClass()).toString)
    obj.eClass().getEAllStructuralFeatures.asScala
      .filter(f => !f.isTransient && !f.isDerived).filter(obj.eIsSet).foreach {
      case ref: EReference =>
        if (ref.isMany) {
          val values = obj.eGet(ref).asInstanceOf[EList[EObject]].asScala
            .map(createValueNode(store, ref.isContainment))
          val rel = /*if (ref.isOrdered)*/ {
            val list = NodeList.from(values, store.graphDb)
            node.createRelationshipTo(list.root, Relations.EReference)
          } /*else {
            val set = NodeSet.empty(store.graphDb, ref.isUnique)
            set ++= values
            node.createRelationshipTo(set.root, Relations.EReference)
          }*/
          rel.setProperty("name", ref.getName)
        } else {
          val feature = createValueNode(store, ref.isContainment)(obj.eGet(ref).asInstanceOf[EObject])
          val rel = node.createRelationshipTo(feature, Relations.EReference)
          rel.setProperty("name", ref.getName)
        }
      case attr: EAttribute =>
        val attrType = attr.getEAttributeType
        val factory = attrType.getEPackage.getEFactoryInstance
        if (attr.isMany) {
          val values = obj.eGet(attr).asInstanceOf[EList[AnyRef]].asScala
            .map(factory.convertToString(attrType, _))
          node.setProperty(attr.getName, values.toArray)
        } else {
          val value = factory.convertToString(attrType, obj.eGet(attr))
          node.setProperty(attr.getName, value)
        }
    }
    node
  })

  override def doSave(outputStream: OutputStream, options: util.Map[_, _]): Unit = {
    if (uri.scheme() == "graph" && outputStream.isInstanceOf[EGraphStoreOutput]) {
      val store = outputStream.asInstanceOf[EGraphStoreOutput].store
      store.graphDb.transaction {
        // delete old resource if existes
        Option(store.graphDb.findNode(Labels.Resource,"uri",uri.host()))
          .foreach(deleteTransitiveOut(_,Relations.Contents,internal.Relations.NEXT_SIBLING,internal.Relations.MEMBER,Relations.EReference))
        // create new resource
        val resource = store.graphDb.createNode(Labels.Resource)
        resource.setProperty("uri",uri.host())
        // write contents
        val list = NodeList.empty(store.graphDb)
        list.appendAll(this.contents.asScala.map(createEObjectNode(store)))
        resource.createRelationshipTo(list.root,Relations.Contents)
      }.get
    } else super.doSave(outputStream,options)
  }
}