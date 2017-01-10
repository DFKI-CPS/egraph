package de.dfki.cps.egraph

import de.dfki.cps.egraph.Neo4j._
import org.eclipse.emf.common.util.{BasicEList, EList, URI}
import org.eclipse.emf.ecore.{EClass, EObject, EStructuralFeature}
import org.neo4j.graphdb.{Direction, DynamicRelationshipType, RelationshipType}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.Buffer
import scala.reflect.ClassTag


/**
 * @author Martin Ring <martin.ring@dfki.de>
 * @since  20.11.2014
 */
class GraphEObject (var node: Node, val graphContainer: GraphEObject, resource: GraphResource, loadOnDemand: Boolean)(implicit connection: Neo4j) extends EObject {
  def uid: String = node.props("uid").asInstanceOf[String]

  // Members declared in org.eclipse.emf.ecore.EObject
  def eAllContents(): org.eclipse.emf.common.util.TreeIterator[org.eclipse.emf.ecore.EObject] = ???

  def eClass(): EClass =
    resource.getResourceSet().getEObject(URI.createURI(node.props("eClass").asInstanceOf[String]), true).asInstanceOf[EClass]

  def eContainer(): org.eclipse.emf.ecore.EObject = graphContainer

  def eContainingFeature(): org.eclipse.emf.ecore.EStructuralFeature = ???

  def eContainmentFeature(): org.eclipse.emf.ecore.EReference = ???

  private var contentCache = Option.empty[GraphEList[GraphEObject]]

  def eContents(): EList[EObject] = getGraphContents.asInstanceOf[EList[EObject]]

  def eCrossReferences(): org.eclipse.emf.common.util.EList[org.eclipse.emf.ecore.EObject] = ???

  def eGet(feature: org.eclipse.emf.ecore.EStructuralFeature, resolve: Boolean): Object =
    if (node.props.get(feature.getName).contains("<null>")) null else
    feature.getEType.getInstanceClassName match {
      case "java.lang.String" => node.props.get(feature.getName).map(s => s.toString).orNull
      case "int" => node.props.get(feature.getName).map(i => new Integer(i.toString.toInt)).orNull
      case "org.eclipse.emf.ecore.EModelElement" =>
        new BasicEList(node.props(feature.getName).toString.split(",").map(uri => resource.getResourceSet().getEObject(URI.createURI(uri), false)).toList.asJava)
      case other =>
        node.props.get(feature.getName).map { uri =>
          resource.getResourceSet().getEObject(URI.createURI(uri.toString), false)
        }.orNull
    }

  def eGet(feature: EStructuralFeature): Object = eGet(feature, resolve = false)

  def eInvoke(x$1: org.eclipse.emf.ecore.EOperation, x$2: org.eclipse.emf.common.util.EList[_]): Object = ???

  def eIsProxy(): Boolean = ???

  def eIsSet(feature: EStructuralFeature): Boolean =
    node.props.isDefinedAt(feature.getName)

  def eResource(): org.eclipse.emf.ecore.resource.Resource = resource

  def eSet(x$1: org.eclipse.emf.ecore.EStructuralFeature, x$2: Any): Unit = connection.transaction { implicit tx =>
    val query = s"START (n) WHERE id(n) = ${node.id} SET n.${x$1.getName} = '${x$2}'"
    Cypher(query).execute()
    node = Cypher(s"MATCH (n) WHERE id(n) = ${node.id} RETURN n as node")().map {
      row => row.get[Node]("node").get
    }.head
  }.get

  def eUnset(x$1: org.eclipse.emf.ecore.EStructuralFeature): Unit = connection.transaction { implicit tx =>
    val query = s"MATCH (n) WHERE id(n) = ${node.id} REMOVE n.${x$1.getName}"
    Cypher(query).execute()
    node = Cypher(s"MATCH (n) WHERE id(n) = ${node.id} RETURN n as node")().map {
      row => row.get[Node]("node").get
    }.head
  }.get

  // Members declared in org.eclipse.emf.common.notify.Notifier                                             
  def eAdapters(): org.eclipse.emf.common.util.EList[org.eclipse.emf.common.notify.Adapter] = ???

  def eDeliver(): Boolean = ???

  def eNotify(x$1: org.eclipse.emf.common.notify.Notification): Unit = ???

  def eSetDeliver(x$1: Boolean): Unit = ???

  def getGraphContents: GraphEList[GraphEObject] = contentCache.getOrElse { connection.transaction { implicit tx =>
    val contentsQuery = s"""
      |MATCH (n:EObject) WHERE id(n) = ${node.id}
      |MATCH (n) -[:CONTENTS]-> (list), p = (list) -[:NEXT_SIBLING*0..]-> (list)
      |RETURN [node IN nodes(p) WHERE node <> list | node ] as items
    """.stripMargin
    val items = Cypher(contentsQuery) { row => row.get[Seq[Node]]("items").get }.flatten
    val list = new GraphEList[GraphEObject](items, x => new GraphEObject(x,this,resource,loadOnDemand))
    contentCache = Some(list)
    list
  }.get }
  
  def insertBefore(el: EObject): Boolean = connection.transaction { implicit tx =>
    implicit val query = mutable.Buffer.empty[String]
    val n = ECoreToGraph.writeEObject(el, resource.rootId)
    val t = connection.graphDb.getNodeById(node.id)
    for {
      br <- t.getRelationships(RelationshipType.withName("NEXT_SIBLING"),Direction.INCOMING).asScala.headOption
      before = br.getStartNode
    } {
      br.delete()
      before.createRelationshipTo(n, RelationshipType.withName("NEXT_SIBLING"))
      n.createRelationshipTo(t, RelationshipType.withName("NEXT_SIBLING"))
      graphContainer.invalidate()
    }
  }.isSuccess
  
  def insertAfter(el: EObject): Boolean = connection.transaction { implicit tx =>
    implicit val query = mutable.Buffer.empty[String]
    val n = ECoreToGraph.writeEObject(el, resource.rootId)
    val t = connection.graphDb.getNodeById(node.id)
    for {
      br <- t.getRelationships(RelationshipType.withName("NEXT_SIBLING"),Direction.OUTGOING).asScala.headOption
      after = br.getEndNode
    } {
      br.delete()
      t.createRelationshipTo(n, RelationshipType.withName("NEXT_SIBLING"))
      n.createRelationshipTo(after, RelationshipType.withName("NEXT_SIBLING"))
      graphContainer.invalidate()
    }
  }.isSuccess

  def remove(): Boolean = connection.transaction { implicit tx =>
    val q = Cypher(s"""
      |MATCH (before) -[:NEXT_SIBLING]-> (this) -[:NEXT_SIBLING]-> (after)
      |WHERE id(this) = ${node.id}
      |MATCH (this) -[r]- ()
      |DELETE r, this
      |CREATE UNIQUE before -[:NEXT_SIBLING]-> after
    """.stripMargin)
    graphContainer.invalidate()
  }.isSuccess

  def append(obj: EObject): Boolean = connection.transaction { implicit tx =>
    implicit val query = mutable.Buffer.empty[String]
    val n = ECoreToGraph.writeEObject(obj, resource.rootId)
    val t = connection.graphDb.getNodeById(node.id)
    for {
      br <- t.getRelationships(RelationshipType.withName("CONTENTS"),Direction.OUTGOING).asScala.headOption
      list = br.getEndNode
      r <- list.getRelationships(RelationshipType.withName("NEXT_SIBLING"),Direction.INCOMING).asScala.headOption
      last = r.getStartNode
    } {
      r.delete()
      last.createRelationshipTo(n,RelationshipType.withName("NEXT_SIBLING"))
      n.createRelationshipTo(list, RelationshipType.withName("NEXT_SIBLING"))
      invalidate()
    }
  }.isSuccess

  def containingFeature: Option[String] = node.props.get("eContainingFeature").map(_.toString)

  def get(feature: String): Option[Any] = for {
    feature <- Option(eClass().getEStructuralFeature(feature))     
    value   <- Option(eGet(feature))
  } yield value

  def plain(feature: String): Option[String] = node.props.get(feature).map(x => x.toString).filter(_ != "<null>")

  def setPlain(feature: String, value: String): Boolean = connection.transaction { implicit tx => Cypher(
    s"""
      |MATCH (n)
      |WHERE id(n) = ${node.id}
      |SET n.$feature = "$value"
    """.stripMargin).execute()
  }.get

  def getAs[T](feature: String)(implicit classTag: ClassTag[T]): Option[T] = get(feature).flatMap {
    case null  => None
    case t: T  => Some(t)
    case other => None
  }

  def apply[T](feature: String): T = get(feature).get.asInstanceOf[T]

  def plainFeature(feature: String): Option[(String,String)] = for {
    value <- node.props.get(feature)
    u = value.toString
  } yield feature -> (if (u.startsWith("graph://")) u.split("/").last else u)

  def feature(feature: String): Option[(String,String)] = for {
    feature <- Option(eClass().getEStructuralFeature(feature))
    value   <- Option(eGet(feature))
  } yield feature.getName -> value.toString

  
  def getEType: Option[GraphEObject] = getAs[GraphEObject]("eType")

  def getEOpposite: Option[GraphEObject] = getAs[GraphEObject]("eOpposite")

  private def getChild(part: String): GraphEObject = part match {
    case "" => getGraphContents.asScala.headOption.orNull
    case part if part.forall(_.isDigit) => getGraphContents.asScala.lift(part.toInt).orNull
    case name if name.startsWith("@") => getGraphContents.asScala.find(_.containingFeature.contains(part.tail)).orNull
    case name => getGraphContents.asScala.find(_.get("name").contains(name)).orNull
  }

  def getGraphObject(fragementParts: Seq[String]): GraphEObject = fragementParts match {
    case Seq(head) => getChild(head)
    case Seq(head, tl @ _*) => Option(getChild(head)).map(_.getGraphObject(tl)).orNull
  }

  def allContent: Iterable[GraphEObject] = Iterable(this) ++ getGraphContents.asScala.flatMap(_.allContent)

  def invalidate(): Unit = {
    contentCache = None
    if (!loadOnDemand) getGraphContents
  }

  invalidate()
}