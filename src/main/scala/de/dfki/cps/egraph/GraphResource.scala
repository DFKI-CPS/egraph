package de.dfki.cps.egraph

import de.dfki.cps.egraph.Neo4j._
import org.eclipse.emf.common.util.{EList, TreeIterator}
import org.eclipse.emf.ecore.EObject
import org.eclipse.emf.ecore.resource.{Resource, ResourceSet}
import org.neo4j.graphdb._

import scala.collection.JavaConversions._
import scala.collection.mutable.Buffer

trait GraphEContent

/**
 * @author Martin Ring <martin.ring@dfki.de>
 */
class GraphResource private[egraph](val rootId: String, resourceSet: HybridResourceSet, loadOnDemand: Boolean)(implicit connection: Neo4j) extends Resource with GraphEContent {
  // Members declared in org.eclipse.emf.common.notify.Notifier
  
  def eAdapters(): org.eclipse.emf.common.util.EList[org.eclipse.emf.common.notify.Adapter] = ???
  
  def eDeliver(): Boolean = ???
  
  def eNotify(x$1: org.eclipse.emf.common.notify.Notification): Unit = ???
  
  def eSetDeliver(x$1: Boolean): Unit = ???                                                                     

  def getAllContents(): TreeIterator[EObject] = ???

  // Members declared in org.eclipse.emf.ecore.resource.Resource
  
  def delete(x$1: java.util.Map[_, _]): Unit = ???
  
  private var contentCache = Option.empty[GraphEList[GraphEObject]]
     
  def getContents(): EList[EObject] = getGraphContents.asInstanceOf[EList[EObject]]
  
  def getEObject(x$1: String): EObject = ???
  
  def getErrors(): EList[org.eclipse.emf.ecore.resource.Resource.Diagnostic] = ???
  
  def getResourceSet(): ResourceSet = 
    resourceSet
  
  def getTimeStamp(): Long = ???
  
  def getURI(): org.eclipse.emf.common.util.URI = ???
  
  def getURIFragment(x$1: org.eclipse.emf.ecore.EObject): String = ???
  
  def getWarnings(): org.eclipse.emf.common.util.EList[org.eclipse.emf.ecore.resource.Resource.Diagnostic] = ???
  
  def isLoaded(): Boolean = ???
  
  def isModified(): Boolean = ???
  
  def isTrackingModification(): Boolean = ???
  
  def load(x$1: java.io.InputStream,x$2: java.util.Map[_, _]): Unit = ???
  
  def load(x$1: java.util.Map[_, _]): Unit = ???
  
  def save(x$1: java.io.OutputStream,x$2: java.util.Map[_, _]): Unit = ???
  
  def save(x$1: java.util.Map[_, _]): Unit = ???
  
  def setModified(x$1: Boolean): Unit = ???
  
  def setTimeStamp(x$1: Long): Unit = ???
  
  def setTrackingModification(x$1: Boolean): Unit = ???
  
  def setURI(x$1: org.eclipse.emf.common.util.URI): Unit = ???
  
  def unload(): Unit = {
    contentCache = None
  }
  
  // additional members
  
  def getID(obj: EObject): String = obj match {
    case obj: GraphEObject => obj.node.props("_ID_").toString
  }

  def rootNode: Option[Node] = {
    val item = connection.transaction(implicit tx => Cypher(
      s"""
        |MATCH (n:Resource { id: "$rootId"})
        |RETURN n as root
      """.stripMargin){ row => row[Node]("root") })
    item.toOption.flatMap(_.headOption)
  }

  def getGraphContents = contentCache.getOrElse {
    val contentsQuery = s"""
      |MATCH (n:Resource { id: "$rootId" }) -[:CONTENTS]-> (list), p = (list) -[:NEXT_SIBLING*0..]-> (list)
      |RETURN [node IN nodes(p) WHERE node <> list | node ] as items
    """.stripMargin
    val items = connection.transaction(implicit tx => Cypher(contentsQuery){ row => row[Seq[Node]]("items") }.flatten ).get
    val list = new GraphEList[GraphEObject](items, x => new GraphEObject(x,null,this,loadOnDemand))
    contentCache = Some(list)
    list
  }

  def append(obj: EObject): Boolean = connection.transaction { implicit tx =>
    implicit val query = Buffer.empty[String]
    val n = ECoreToGraph.writeEObject(obj, rootId)
    for {
      t <- Option(connection.graphDb.findNode(Label.label("Resource"), "id", rootId))
      br <- t.getRelationships(RelationshipType.withName("CONTENTS"),Direction.OUTGOING).headOption
      list = br.getEndNode
      r <- list.getRelationships(RelationshipType.withName("NEXT_SIBLING"),Direction.INCOMING).headOption
      last = r.getStartNode
    } {
      r.delete()
      last.createRelationshipTo(n,RelationshipType.withName("NEXT_SIBLING"))
      n.createRelationshipTo(list, RelationshipType.withName("NEXT_SIBLING"))
      invalidate()
    }
  }.isSuccess

  private def getChild(part: String): GraphEObject = part match {
    case "" => getGraphContents.headOption.orNull
    case part if part.forall(_.isDigit) => getGraphContents.lift(part.toInt).orNull
    case name => getGraphContents.find(_.get("name").contains(name)).orNull
  }

  def getGraphObject(fragementParts: Seq[String]): GraphEObject = fragementParts match {
    case Seq(head) => getChild(head)
    case Seq(head, tl @ _*) => getChild(head).getGraphObject(tl)
  }

  def allContent: Iterable[GraphEObject] = getGraphContents.flatMap(_.allContent)

  def invalidate(): Unit = {
    contentCache = None
    if (!loadOnDemand) getGraphContents
  }

  invalidate()
}