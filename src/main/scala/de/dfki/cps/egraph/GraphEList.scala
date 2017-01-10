package de.dfki.cps.egraph

import org.eclipse.emf.common.util.EList

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.Buffer


/**
 * @author Martin Ring <martin.ring@dfki.de>
 * @since  20.11.2014
 */
class GraphEList[T] private [egraph] (q: Seq[Node], f: Node => T)(implicit connection: Neo4j) extends EList[T] {
  val qi = mutable.Buffer(q:_*)
  
  private lazy val buffer = Buffer(q.map(f) :_*)
  
  // Members declared in org.eclipse.emf.common.util.EList
  def move(x$1: Int,x$2: Int): T = ???
  def move(x$1: Int,x$2: T): Unit = ???

  // Members declared in java.util.List
  def add(x$1: Int,x$2: T): Unit = ???
  def add(x$1: T): Boolean = ???
  def addAll(x$1: Int,x$2: java.util.Collection[_ <: T]): Boolean = ???
  def addAll(x$1: java.util.Collection[_ <: T]): Boolean = ???
  def clear(): Unit = ???
  def contains(x$1: Any): Boolean = buffer.contains(x$1)
  def containsAll(x$1: java.util.Collection[_]): Boolean = x$1.asScala.forall(buffer.contains)
  def get(x$1: Int): T = f(q(x$1))
  def indexOf(x$1: Any): Int = buffer.indexOf(x$1)
  def isEmpty(): Boolean = buffer.isEmpty
  def iterator(): java.util.Iterator[T] = q.map(f).iterator.asJava
  def lastIndexOf(x$1: Any): Int = buffer.lastIndexOf(x$1)
  def listIterator(x$1: Int): java.util.ListIterator[T] = buffer.asJava.listIterator(x$1)
  def listIterator(): java.util.ListIterator[T] = buffer.asJava.listIterator()
  def remove(x$1: Int): T = connection.transaction { implicit tx =>
    val node = qi(x$1)
    // FIXME: Make this generic (i.e. remove explicit dependencies on CONTENTS and ORIGIN of nodes)
    val query = Cypher(s"""
      |MATCH (root:EList)-[:NEXT_SIBLING*0..]->(before),
      |      (before) -[delBefore:NEXT_SIBLING]-> (del) -[delAfter:NEXT_SIBLING]-> (after), 
      |      (after) -[:NEXT_SIBLING*0..]-> (root),
      |      (del) -[r:CONTENTS]-> (contents),
      |      (del) <-[s:ORIGIN]- (semantic)
      |WHERE id(del) = ${node.id}
      |CREATE UNIQUE (before)-[:NEXT_SIBLING]->(after)
      |DELETE r, s, del, delBefore, delAfter
    """.stripMargin)    
    query.execute()
    qi.remove(x$1)
    buffer.remove(x$1)
  }.get
  def remove(x$1: Any): Boolean = delete(x$1)
  def delete(x$1: Any): Boolean = x$1 match {
    case g: GraphEObject => {
      val i = qi.indexWhere { _.id == g.node.id }      
      val x: T = remove(i)
      true
    }
  }
  def removeAll(x$1: java.util.Collection[_]): Boolean = ???
  def retainAll(x$1: java.util.Collection[_]): Boolean = ???
  def set(x$1: Int,x$2: T): T = ???
  def size(): Int = buffer.size
  def subList(x$1: Int,x$2: Int): java.util.List[T] = buffer.asJava.subList(x$1, x$2)
  def toArray[T](x$1: Array[T with Object]): Array[T with Object] = ???
  def toArray(): Array[Object] = ???
  def map[U](g: T => U): GraphEList[U] = new GraphEList[U](q, f andThen g)
}