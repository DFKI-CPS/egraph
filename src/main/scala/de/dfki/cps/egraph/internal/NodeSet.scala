package de.dfki.cps.egraph.internal

import org.neo4j.graphdb.{Direction, GraphDatabaseService, Node}

import scala.collection.mutable
import scala.collection.JavaConverters._

/**
  * @author Martin Ring <martin.ring@dfki.de>
  */
class NodeSet(val root: Node, unique: Boolean = true) extends mutable.Set[Node] {
  def +=(elem: Node): NodeSet.this.type = {
    if (!unique || !contains(elem)) root.createRelationshipTo(elem, Relations.MEMBER)
    this
  }

  def -=(elem: Node): NodeSet.this.type = {
    val it = elem.getRelationships(Direction.INCOMING, Relations.MEMBER)
          .iterator().asScala.filter(_.getStartNode == root)
    if (unique) it.foreach(_.delete())
    else if (it.hasNext) it.next().delete()
    this
  }

  def contains(elem: Node): Boolean =
    elem.getRelationships(Direction.INCOMING, Relations.MEMBER)
      .asScala
      .exists(_.getStartNode == root)

  def iterator: Iterator[Node] =
    root.getRelationships(Direction.OUTGOING, Relations.MEMBER)
      .iterator()
      .asScala
      .map(_.getEndNode)

  override def size: Int =
    root.getDegree(Relations.MEMBER,Direction.OUTGOING)
}

object NodeSet {
  def empty(graph: GraphDatabaseService, unique: Boolean = true): NodeSet = {
    new NodeSet(graph.createNode(Labels.SET), unique)
  }
}