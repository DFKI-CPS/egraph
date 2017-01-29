package de.dfki.cps.egraph

import java.io.File
import java.util

import de.dfki.cps.egraph.stools.Diff
import de.dfki.cps.secore.SResource
import de.dfki.cps.specific.sysml
import de.dfki.cps.stools.STools
import de.dfki.cps.stools.similarityspec.SimilaritySpec
import org.eclipse.emf.common.util.URI
import org.eclipse.emf.ecore.resource.impl.{ResourceImpl, ResourceSetImpl}
import org.eclipse.uml2.uml.UMLPackage
import org.scalatest.{FunSuite, Matchers}

import scala.io.Source

/**
  * @author Martin Ring <martin.ring@dfki.de>
  */
class Test extends FunSuite with Matchers {
  test("bla") {
    val rs = new ResourceSetImpl
    val dir = new File("test")
    if (!dir.exists()) dir.mkdirs()
    val store = new EGraphStore(dir)
    store.attach(rs)
    sysml.Synthesis.prepareLibrary(rs)
    val modelA = sysml.Model.load(getClass.getClassLoader.getResource("modelA.sysml").toURI,"example")(rs)
    val modelB = sysml.Model.load(getClass.getClassLoader.getResource("modelB.sysml").toURI,"example")(rs)
    val modelB2 = sysml.Model.load(getClass.getClassLoader.getResource("modelB.sysml").toURI,"example")(rs)
    val resA = new ResourceImpl()
    val resB = rs.createResource(URI.createURI("graph://test"))
    val resB2 = new ResourceImpl()
    resA.getContents.addAll(modelA.getContents)
    resB.getContents.addAll(modelB.getContents)
    resB2.getContents.addAll(modelB2.getContents)

    val stool = de.dfki.cps.secore.stools.getSTool("specific")

    resB.save(new util.HashMap)
    stool.sdiff(new SResource(resB), new SResource(resB2))
        .entries shouldBe empty

    resB.unload()
    resB.load(new util.HashMap)

    val script = stool.sdiff(new SResource(resB), new SResource(resA))
    script.entries.foreach(println)

    println("APPLYING SCRIPT")

    Diff.applyDiff(resB.asInstanceOf[GraphResource],script)


    resB.unload()
    resB.load(new util.HashMap)

    val script2 = stool.sdiff(new SResource(resB), new SResource(resA))
    assert(script2.entries.isEmpty)

  }
}
