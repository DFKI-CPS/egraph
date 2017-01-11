package de.dfki.cps.egraph.internal

import org.neo4j.graphdb.Node

/**
  * @author Martin Ring <martin.ring@dfki.de>
  */
class GraphList(root: Node) extends Seq[Node] {
  def length: Int = ???
  def apply(idx: Int): Node = ???
  def iterator: Iterator[Node] = ???
}
