package de.dfki.cps.egraph.stools

import de.dfki.cps.egraph.GraphResource
import de.dfki.cps.secore.SObject
import de.dfki.cps.stools.editscript.SEditScript
import de.dfki.cps.stools.editscript.SEditScript.SEditScriptEntry

/**
  * @author Martin Ring <martin.ring@dfki.de>
  */
object Diff {
  def applyDiff(resource: GraphResource, diff: SEditScript) = diff.entries.foreach {
    case (o: SObject, entry: SEditScriptEntry) =>
      resource.getNode(o.underlying).fold {
        println("error: no graph node exists for object " + o.underlying)
      } { node =>
        node.getDegree
      }
    case other => println("error: unhandled SEditScriptEntry: " + other._2)
  }
}