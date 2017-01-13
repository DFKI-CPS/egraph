package de.dfki.cps.egraph

import org.neo4j.graphdb.Label

/**
  * @author Martin Ring <martin.ring@dfki.de>
  */
object Labels {
  val Resource = Label.label("Resource")
  val EObject = Label.label("EObject")
  val EReference = Label.label("EReference")
  val EList = Label.label("EList")
  val Literal = Label.label("Literal")
}
