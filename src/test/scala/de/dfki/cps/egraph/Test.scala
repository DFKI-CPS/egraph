package de.dfki.cps.egraph

import java.io.File
import java.util

import de.dfki.cps.specific.sysml
import org.eclipse.emf.common.util.URI
import org.eclipse.emf.ecore.resource.impl.ResourceSetImpl
import org.scalatest.FunSuite

/**
  * @author Martin Ring <martin.ring@dfki.de>
  */
class Test extends FunSuite {
  test("bla") {
    val rs = new ResourceSetImpl
    val dir = new File("test")
    if (!dir.exists()) dir.mkdirs()
    val store = new EGraphStore(dir)
    store.attach(rs)
    sysml.Synthesis.prepareLibrary(rs)
    val model = sysml.Model.load(getClass.getClassLoader.getResource("example.sysml").toURI)(rs)
    val res = rs.createResource(URI.createURI("graph://test"))
    res.getContents.add(model)
    res.save(new util.HashMap)
    res.unload()
    res.load(new util.HashMap)
  }
}
