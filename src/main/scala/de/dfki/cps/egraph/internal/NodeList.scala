package de.dfki.cps.egraph.internal

import de.dfki.cps.egraph.{Labels, Relations}
import org.neo4j.graphdb.{Direction, GraphDatabaseService, Node}

import scala.collection.mutable

/**
  * @author Martin Ring <martin.ring@dfki.de>
  */
class NodeList(root: Node) extends mutable.Buffer[Node] {
  if (!root.hasRelationship(Relations.NextSibling))
    root.createRelationshipTo(root,Relations.NextSibling)

  def apply(n: Int): Node = {
    val i = iterator.drop(n)
    if (i.hasNext) i.next() else throw new IndexOutOfBoundsException(s"GraphList($n)")
  }

  def update(n: Int, newelem: Node): Unit = {
    val old = apply(n)
    val in = old.getSingleRelationship(Relations.NextSibling,Direction.INCOMING)
    val before = in.getStartNode
    val out = old.getSingleRelationship(Relations.NextSibling,Direction.OUTGOING)
    val after = out.getEndNode
    in.delete()
    out.delete()
    before.createRelationshipTo(newelem,Relations.NextSibling)
    newelem.createRelationshipTo(after,Relations.NextSibling)
  }

  def length: Int = iterator.length

  def +=(elem: Node): NodeList.this.type = {
    val in = root.getSingleRelationship(Relations.NextSibling,Direction.INCOMING)
    val before = in.getStartNode
    in.delete()
    before.createRelationshipTo(elem,Relations.NextSibling)
    elem.createRelationshipTo(root,Relations.NextSibling)
    this
  }

  def clear(): Unit = {
    iterator.foreach { node =>
      val in = node.getSingleRelationship(Relations.NextSibling, Direction.INCOMING)
      val out = node.getSingleRelationship(Relations.NextSibling, Direction.OUTGOING)
      in.delete()
      out.delete()
    }
    root.createRelationshipTo(root,Relations.NextSibling)
  }

  def +=:(elem: Node): NodeList.this.type = {
    val out = root.getSingleRelationship(Relations.NextSibling,Direction.OUTGOING)
    val after = out.getEndNode
    out.delete()
    root.createRelationshipTo(elem,Relations.NextSibling)
    elem.createRelationshipTo(after,Relations.NextSibling)
    this
  }

  def insertAll(n: Int, elems: Traversable[Node]): Unit = {
    val old = apply(n)
    val in = old.getSingleRelationship(Relations.NextSibling,Direction.INCOMING)
    val before = in.getStartNode
    val out = old.getSingleRelationship(Relations.NextSibling,Direction.OUTGOING)
    val after = out.getEndNode
    in.delete()
    out.delete()
    val last = elems.foldLeft(before) {
      case (before,next) =>
        before.createRelationshipTo(next,Relations.NextSibling)
        next
    }
    last.createRelationshipTo(after,Relations.NextSibling)
  }

  def remove(n: Int): Node = {
    val node = apply(n)
    val in = apply(n).getSingleRelationship(Relations.NextSibling,Direction.INCOMING)
    val before = in.getStartNode
    val out = apply(n).getSingleRelationship(Relations.NextSibling,Direction.OUTGOING)
    val after = out.getEndNode
    in.delete()
    out.delete()
    before.createRelationshipTo(after,Relations.NextSibling)
    node
  }

  def iterator: Iterator[Node] = new Iterator[Node] {
    var current = root.getSingleRelationship(Relations.NextSibling,Direction.OUTGOING).getEndNode
    def hasNext: Boolean = current.getId != root.getId
    def next(): Node = {
      val result = current
      current = current.getSingleRelationship(Relations.NextSibling,Direction.OUTGOING).getEndNode
      result
    }
  }
}