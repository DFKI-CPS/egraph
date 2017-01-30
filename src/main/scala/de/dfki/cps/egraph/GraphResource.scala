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
import org.eclipse.emf.ecore.{EAttribute, EClass, EObject, EReference}
import org.neo4j.graphdb.{Direction, GraphDatabaseService, Node}

import scala.beans.BooleanBeanProperty
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.duration.Duration

/**
  * @author Martin Ring <martin.ring@dfki.de>
  */
class GraphResource(uri: URI) extends ResourceImpl(uri) {
  implicit val defaultTimeout = Duration.Inf

  private var rootNode = Option.empty[Node]
  private val nodeMap = mutable.Map.empty[EObject,Node]
  private val objectMap = mutable.Map.empty[Node,EObject]

  def root: Option[Node] = rootNode
  def getNode(obj: EObject) = nodeMap.get(obj)
  def getObject(node: Node) = objectMap.get(node)

  def adapt(obj: EObject, node: Node) = {
    val adapter = new AdapterImpl {
      override def notifyChanged(msg: Notification): Unit = {
        println(msg)
      }
    }
    obj.eAdapters().add(adapter)
  }

  private def readValueNode(node: Node): AnyRef = {
    Option(node.getSingleRelationship(Relations.EReferenceLink,Direction.OUTGOING)).fold {
      if (node.hasLabel(Labels.EObject)) readEObjectNode(node)
      else if (node.hasLabel(Labels.EReference)) {
        val uri = URI.createURI(node.getProperty("uri").asInstanceOf[String])
        val other = if (uri.hasAuthority)
          resourceSet.getEObject(uri,true)
        else getEObject(uri.toString)
        nodeMap.get(other).foreach { local =>
          node.createRelationshipTo(local, Relations.EReferenceLink)
        }
        if (other == null) println("could not resolve " + uri)
        other
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
          if (!ref.isContainment) deferredResolutions += { () =>
            if (ref.isMany) {
              val values = if (ref.isOrdered) {
                new NodeList(node).map(readValueNode).asJavaCollection
              } else {
                new NodeSet(node, ref.isUnique).map(readValueNode).asJavaCollection
              }
              val elist = x.eGet(ref).asInstanceOf[EList[AnyRef]]
              elist.addAll(values)
            } else {
              x.eSet(ref, readValueNode(node))
            }
          } else {
            if (ref.isMany) {
              val values = if (ref.isOrdered) {
                new NodeList(node).map(readValueNode).asJavaCollection
              } else {
                new NodeSet(node, ref.isUnique).map(readValueNode).asJavaCollection
              }
              val elist = x.eGet(ref).asInstanceOf[EList[AnyRef]]
              elist.addAll(values)
            } else {
              x.eSet(ref, readValueNode(node))
            }
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
    x
  })

  private val deferredResolutions = mutable.Buffer.empty[() => Unit]

  override def doLoad(inputStream: InputStream, options: util.Map[_, _]): Unit = {
    if (uri.scheme() == "graph" && inputStream.isInstanceOf[EGraphStoreInput]) {
      val store = inputStream.asInstanceOf[EGraphStoreInput].store
      this.nodeMap.clear()
      this.objectMap.clear()
      this.deferredResolutions.clear()
      store.graphDb.transaction {
        val resource = Option(store.graphDb.findNode(Labels.Resource,"uri",uri.host()))
        resource.foreach { root =>
          this.contents.clear()
          val contents = new NodeList(root.getSingleRelationship(Relations.Contents,Direction.OUTGOING).getEndNode)
          this.contents.addAll(contents.map(readEObjectNode).asJava)
        }
        rootNode = resource
        deferredResolutions.foreach(_())
      }.get
    } else super.doLoad(inputStream,options)
  }

  private def createValueNode(graphDb: GraphDatabaseService, containment: Boolean)(obj: EObject): Node = obj match {
    case eObj: EObject if containment => createEObjectNode(graphDb)(eObj)
    case ref: EObject =>
      val node = graphDb.createNode(Labels.EReference)
      if (ref.eResource() == this) {
        val frag = getURIFragment(ref)
        node.setProperty("uri",frag)
        val refNode = createEObjectNode(graphDb)(ref)
        node.createRelationshipTo(refNode,Relations.EReferenceLink)
      } else {
        val uri = EcoreUtil.getURI(ref)
        node.setProperty("uri",uri.toString)
      }
      node
  }

  def insertEObjectNode(graphDb: GraphDatabaseService)(obj: EObject): Node = {
    assert(!nodeMap.isDefinedAt(obj))
    val node = graphDb.createNode(Labels.EObject)
    nodeMap += obj -> node
    node.setProperty("eClass", EcoreUtil.getURI(obj.eClass()).toString)
    obj.eClass().getEAllStructuralFeatures.asScala
      .filter(f => !f.isTransient && !f.isDerived && !f.isVolatile).filter(obj.eIsSet).foreach {
      case ref: EReference =>
        if (ref.isMany) {
          val objs = obj.eGet(ref).asInstanceOf[EList[EObject]].asScala
          val values = if (ref.isContainment) objs.map(insertEObjectNode(graphDb))
          else objs.map { vo =>
            val node = graphDb.createNode(Labels.EReference)
            val uri = if (vo.eResource() == obj.eResource())
              obj.eResource().getURIFragment(vo)
            node.setProperty("uri", uri)
            node
          }
          val rel = if (ref.isOrdered) {
            val list = NodeList.from(values, graphDb)
            node.createRelationshipTo(list.root, Relations.EReference)
          } else {
            val set = NodeSet.empty(graphDb, ref.isUnique)
            set ++= values
            node.createRelationshipTo(set.root, Relations.EReference)
          }
          rel.setProperty("name", ref.getName)
        } else {
          val feature = if (ref.isContainment)
            insertEObjectNode(graphDb)(obj.eGet(ref).asInstanceOf[EObject])
          else {
            val node = graphDb.createNode(Labels.EReference)
            val vo = obj.eGet(ref).asInstanceOf[EObject]
            val uri = if (vo.eResource() == obj.eResource())
              obj.eResource().getURIFragment(vo)
            else EcoreUtil.getURI(vo).toString
            node.setProperty("uri", uri)
            node
          }
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
  }

  def createEObjectNode(graphDb: GraphDatabaseService)(obj: EObject): Node = nodeMap.getOrElse(obj, {
    val node = graphDb.createNode(Labels.EObject)
    nodeMap += obj -> node
    objectMap += node -> obj
    node.setProperty("eClass", EcoreUtil.getURI(obj.eClass()).toString)
    obj.eClass().getEAllStructuralFeatures.asScala
      .filter(f => !f.isTransient && !f.isDerived && !f.isVolatile).filter(obj.eIsSet).foreach {
      case ref: EReference =>
        if (ref.isMany) {
          val values = obj.eGet(ref).asInstanceOf[EList[EObject]].asScala
            .map(createValueNode(graphDb, ref.isContainment))
          val rel = if (ref.isOrdered) {
            val list = NodeList.from(values, graphDb)
            node.createRelationshipTo(list.root, Relations.EReference)
          } else {
            val set = NodeSet.empty(graphDb, ref.isUnique)
            set ++= values
            node.createRelationshipTo(set.root, Relations.EReference)
          }
          rel.setProperty("name", ref.getName)
        } else {
          val feature = createValueNode(graphDb, ref.isContainment)(obj.eGet(ref).asInstanceOf[EObject])
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
        list.appendAll(this.contents.asScala.map(createEObjectNode(store.graphDb)))
        resource.createRelationshipTo(list.root,Relations.Contents)
        rootNode = Some(resource)
      }.get
    } else super.doSave(outputStream,options)
  }
}