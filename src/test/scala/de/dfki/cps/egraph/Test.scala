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
  def equal(a: Resource, b: Resource) = {
    val sa = new SResource(a)
    val sb = new SResource(b)
    val stool = new STools(new java.io.File(getClass.getClassLoader.getResource("ecore.simeq").getFile))
    stool.getSTool("ecore").sdiff(sa,sb).entries shouldBe empty
  }

  test("bla") {
    val rs = new ResourceSetImpl
    val dir = new File("test")
    if (!dir.exists()) dir.mkdirs()
    val store = new EGraphStore(dir)
    store.attach(rs)
    sysml.Synthesis.prepareLibrary(rs)
    val modelA = sysml.Model.load(getClass.getClassLoader.getResource("example.sysml").toURI,"example")(rs)
    val modelB = sysml.Model.load(getClass.getClassLoader.getResource("example.sysml").toURI,"example")(rs)
    val res = rs.createResource(URI.createURI("graph://test"))
    val res2 = new ResourceImpl()
    res.getContents.addAll(modelA.getContents)
    res2.getContents.addAll(modelB.getContents)
    res.save(new util.HashMap)
    res.unload()
    val res3= rs.getResource(URI.createURI("graph://test"),true)
    res.load(new util.HashMap)
    equal(res2,res3)
    store.graphDb.transaction {
      println(store.graphDb.getAllNodes.stream().count() + " nodes")
      println(store.graphDb.getAllRelationships.stream().count() + " relations")
    }
    res.delete(new util.HashMap)
    store.graphDb.transaction {
      println(store.graphDb.getAllNodes.stream().count() + " nodes")
      println(store.graphDb.getAllRelationships.stream().count() + " relations")
    }
  }

  test("blub") {
    val rs = new ResourceSetImpl
    val dir = new File("test")
    if (!dir.exists()) dir.mkdirs()
    val store = new EGraphStore(dir)
    store.attach(rs)
    val res = rs.createResource(URI.createURI("graph://test"))
    val f = EcoreFactory.eINSTANCE
    val pkg = f.createEPackage()
    res.getContents.add(pkg)
    res.save(new util.HashMap)
    res.unload()
    res.load(new util.HashMap)
    val p = res.getContents.get(0).asInstanceOf[EPackage]
    val c = f.createEClass()
    val c2 = f.createEClass()
    p.setName("Hallo")
    p.setName(null)
    p.getEClassifiers.add(c)
    p.getEClassifiers.remove(c)
    import collection.JavaConverters._
    p.getEClassifiers.addAll(List(c,c2).asJava)
  }

  test("foo") {
    val f = EcoreFactory.eINSTANCE
    val res = new ResourceImpl
    val x = f.createEClass()

    val adapter = new AdapterImpl {
      override def notifyChanged(msg: Notification): Unit = {
        super.notifyChanged(msg)
        msg.getEventType match {
          case Notification.ADD_MANY =>
            println("add all")
          case Notification.ADD =>
            println("add " + msg.getNewValue)
          case Notification.MOVE =>
            println("move")
          case Notification.REMOVE_MANY =>
            println("remove all")
          case Notification.REMOVE =>
            println("remove")
          case other =>
            println("unhandled " + msg)
        }
      }
    }

    res.eAdapters().add(adapter)

    res.getContents.add(x)

    x.setAbstract(true)
    val op = f.createEOperation()
    x.getEOperations.add(op)
    op.setName("Hallo")
  }
}
