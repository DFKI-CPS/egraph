package de.dfki.cps.egraph

import java.io.File
import java.util

import de.dfki.cps.secore.SResource
import de.dfki.cps.specific.sysml
import de.dfki.cps.stools.STools
import org.eclipse.emf.common.util.URI
import org.eclipse.emf.ecore.resource.impl.{ResourceImpl, ResourceSetImpl}
import org.scalatest.{FunSuite, Matchers}
import internal.Util._
import org.eclipse.emf.common.notify.Notification
import org.eclipse.emf.common.notify.impl.AdapterImpl
import org.eclipse.emf.ecore.resource.Resource
import org.eclipse.emf.ecore.{EAttribute, EObject, EPackage, EReference, EcoreFactory}

import scala.collection.JavaConverters._

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

    resB.save(new util.HashMap)
    de.dfki.cps.secore.stools.getSTool("uml").sdiff(new SResource(resB), new SResource(resB2))
        .entries shouldBe empty

    resB.unload()
    resB.load(new util.HashMap)

    de.dfki.cps.secore.stools.getSTool("uml").sdiff(new SResource(resB), new SResource(resB2))
      .entries shouldBe empty


  }
}
