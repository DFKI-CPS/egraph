package de.dfki.cps.egraph.internal

import org.neo4j.graphdb.{Direction, GraphDatabaseService, Node}

import scala.collection.mutable

/**
  * @author Martin Ring <martin.ring@dfki.de>
  */
class NodeList(val root: Node) extends mutable.Buffer[Node] {
  root.getDegree(Relations.NEXT_SIBLING, Direction.BOTH) match {
    case 0 =>
      root.createRelationshipTo(root, Relations.NEXT_SIBLING)
      length = 0
    case 1 => // ok
    case 2 => // ok
    case n => throw new IllegalArgumentException(s"root node has degree $n. may only be 0, 1 or 2")
  }

  def apply(n: Int): Node = {
    if (n < length / 2) {
      val i = iterator.drop(n)
      i.next()
    } else if (n < length) {
      val i = reverseIterator.drop(length - 1 - n)
      i.next()
    } else throw new IndexOutOfBoundsException
  }

  def update(n: Int, newelem: Node): Unit = {
    val old = apply(n)
    val in = old.getSingleRelationship(Relations.NEXT_SIBLING,Direction.INCOMING)
    val before = in.getStartNode
    val out = old.getSingleRelationship(Relations.NEXT_SIBLING,Direction.OUTGOING)
    val after = out.getEndNode
    in.delete()
    out.delete()
    before.createRelationshipTo(newelem,Relations.NEXT_SIBLING)
    newelem.createRelationshipTo(after,Relations.NEXT_SIBLING)
  }

  private def length_=(value: Int): Unit = {
    root.setProperty("length",value)
  }

  def length: Int = root.getProperty("length").asInstanceOf[Int]

  def +=(elem: Node): NodeList.this.type = {
    val in = root.getSingleRelationship(Relations.NEXT_SIBLING,Direction.INCOMING)
    val before = in.getStartNode
    in.delete()
    before.createRelationshipTo(elem,Relations.NEXT_SIBLING)
    elem.createRelationshipTo(root,Relations.NEXT_SIBLING)
    length += 1
    this
  }

  def clear(): Unit = {
    iterator.foreach { node =>
      val in = node.getSingleRelationship(Relations.NEXT_SIBLING, Direction.INCOMING)
      in.delete()
    }
    val in = root.getSingleRelationship(Relations.NEXT_SIBLING, Direction.INCOMING)
    in.delete()
    root.createRelationshipTo(root,Relations.NEXT_SIBLING)
    length = 0
  }

  def +=:(elem: Node): NodeList.this.type = {
    val out = root.getSingleRelationship(Relations.NEXT_SIBLING,Direction.OUTGOING)
    val after = out.getEndNode
    out.delete()
    root.createRelationshipTo(elem,Relations.NEXT_SIBLING)
    elem.createRelationshipTo(after,Relations.NEXT_SIBLING)
    length += 1
    this
  }

  def insertAll(n: Int, elems: Traversable[Node]): Unit = {
    val before = if (n == 0) root else apply(n - 1)
    val out = before.getSingleRelationship(Relations.NEXT_SIBLING,Direction.OUTGOING)
    val after = out.getEndNode
    out.delete()
    val last = elems.foldLeft(before) {
      case (before,next) =>
        before.createRelationshipTo(next,Relations.NEXT_SIBLING)
        next
    }
    last.createRelationshipTo(after,Relations.NEXT_SIBLING)
    length += elems.size
  }

  def remove(n: Int): Node = {
    val node = apply(n)
    val in = apply(n).getSingleRelationship(Relations.NEXT_SIBLING,Direction.INCOMING)
    val before = in.getStartNode
    val out = apply(n).getSingleRelationship(Relations.NEXT_SIBLING,Direction.OUTGOING)
    val after = out.getEndNode
    in.delete()
    out.delete()
    before.createRelationshipTo(after,Relations.NEXT_SIBLING)
    length -= 1
    node
  }

  def removeAll(p: Node => Boolean) = {
    // TODO: Can be made easily more efficient
    val marked = iterator.filter(p).toSeq
    marked.foreach(x => remove(indexOf(x)))
  }

  def iterator: Iterator[Node] = new Iterator[Node] {
    var current = root.getSingleRelationship(Relations.NEXT_SIBLING,Direction.OUTGOING).getEndNode
    def hasNext: Boolean = current.getId != root.getId
    def next(): Node = {
      val result = current
      current = current.getSingleRelationship(Relations.NEXT_SIBLING,Direction.OUTGOING).getEndNode
      result
    }
  }

  override def reverseIterator: Iterator[Node] = new Iterator[Node] {
    var current = root.getSingleRelationship(Relations.NEXT_SIBLING,Direction.INCOMING).getStartNode
    def hasNext: Boolean = current.getId != root.getId
    def next(): Node = {
      val result = current
      current = current.getSingleRelationship(Relations.NEXT_SIBLING,Direction.INCOMING).getStartNode
      result
    }
  }
}

object NodeList {
  def empty(graph: GraphDatabaseService) =
    new NodeList(graph.createNode(Labels.LIST))
  def apply(first: Node, other: Node*) =
    new NodeList(first.getGraphDatabase.createNode(Labels.LIST)) ++= first +: other
  def from(i: Iterable[Node], graph: GraphDatabaseService) =
    empty(graph) ++= i
}