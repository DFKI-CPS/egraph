package de.dfki.cps.egraph.stools

import de.dfki.cps.egraph.internal.{NodeList, NodeSet}
import de.dfki.cps.egraph.{GraphResource, Labels, Relations}
import de.dfki.cps.secore._
import de.dfki.cps.stools.SAtomicString
import de.dfki.cps.stools.editscript._
import org.eclipse.emf.common.util.{EList, URI}
import org.eclipse.emf.ecore.EObject
import org.eclipse.emf.ecore.util.EcoreUtil
import org.neo4j.graphdb.Direction

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * @author Martin Ring <martin.ring@dfki.de>
  */
object Diff {
  def applyDiff(resource: GraphResource, editScript: SEditScript) = {
    import de.dfki.cps.egraph.internal.Util._
    val graph = resource.root.get.getGraphDatabase
    var deferredResolutions: mutable.Buffer[() => Unit] = mutable.Buffer.empty
    graph.transaction {
      editScript.entries.foreach {
        case (res: SResource, entry) =>
          assert(entry.appendAnnotations.isEmpty)
          assert(entry.removeAnnotations.isEmpty)
          assert(entry.updateAnnotations.isEmpty)
        case (obj: SObject, entry) =>
          val node = resource.getNode(obj.underlying).get
          val refs = node.getRelationships(Direction.OUTGOING, Relations.EReference).asScala.map { rel =>
            rel.getProperty("name").asInstanceOf[String] -> rel.getEndNode
          }.toMap
          assert(entry.insertAfter.isEmpty)
          assert(entry.insertBefore.isEmpty)
          assert(entry.appendElements.isEmpty)
          assert(entry.removeElements.isEmpty)
          assert(entry.replaceElements.isEmpty)
          entry.updateAnnotations.foreach {
            case UpdateAnnotation(_, o: SAttributeValue, n: SAttributeValue) =>
              node.setProperty(n.underlying.getName, n.value)
            case UpdateAnnotation(_, o: SReferenceValue, n: SReferenceValue) =>
              refs.get(o.underlying.getName).foreach { other =>
                other.getRelationships(Direction.OUTGOING,Relations.EReferenceLink)
                  .asScala.foreach(_.delete())
                other.setProperty("uri", n.value)
              }
          }
          entry.removeAnnotations.foreach {
            case RemoveAnnotations(ref, annons) =>
              annons.asScala.foreach {
                case s: SAttributeValue =>
                  node.setProperty(s.underlying.getName, null)
                case s: SReferenceValue =>
                  node.getRelationships(Direction.OUTGOING,Relations.EReference)
                      .asScala.find(_.getProperty("name") == s.underlying.getName)
                    .foreach(_.delete())
              }
          }
          entry.appendAnnotations.foreach {
            case AppendAnnotations(ref, annons) =>
              annons.asScala.foreach {
                case s: SAttributeValue =>
                  node.setProperty(s.underlying.getName, s.value)
                case r: SReferenceValue =>
                  val refNode = graph.createNode(Labels.EReference)
                  refNode.setProperty("uri",r.value)
                  val rel = node.createRelationshipTo(refNode,Relations.EReference)
                  rel.setProperty("name",r.underlying.getName)
              }
          }
        case (attr: SAttribute, entry) =>
          val node = resource.getNode(attr.parent.underlying).get
          assert(entry.appendAnnotations.isEmpty)
          assert(entry.removeAnnotations.isEmpty)
          assert(entry.updateAnnotations.isEmpty)
          entry.removeElements.foreach {
            case RemoveElements(_, elems) =>
              val o = node.getProperty(attr.underlying.getName).asInstanceOf[Array[String]]
              elems.asScala.foreach {
                case elem: SLiteral => o.update(elem.index,null)
              }
              val n = Array(o.toSeq.filter(_ != null) :_*)
              node.setProperty(attr.underlying.getName,n)
          }
          entry.appendElements.foreach {
            case AppendElements(_, elems) =>
              val o = node.getProperty(attr.underlying.getName).asInstanceOf[Array[String]]
              val n = Array(o.toSeq ++ elems.asScala.map {
                case v: SLiteral => v.label
              } :_*)
              node.setProperty(attr.underlying.getName,n)
          }
          /*entry.insertBefore.foreach {
            case (_, InsertBefore(ref: SLiteral, elems)) =>
              val values = elems.asScala.map {
                case s: SLiteral =>
                  s.underlying
              }
              ref.parent.parent.underlying.eGet(ref.parent.underlying).asInstanceOf[EList[AnyRef]]
                .addAll(ref.index, values.asJava)
          }
          entry.insertAfter.foreach {
            case (_, InsertAfter(ref: SLiteral, elems)) =>
              val values = elems.asScala.map {
                case s: SLiteral =>
                  s.underlying
              }
              ref.parent.parent.underlying.eGet(ref.parent.underlying).asInstanceOf[EList[AnyRef]]
                .addAll(ref.index + 1, values.asJava)
          }*/
        case (ref: SReference, entry) if ref.underlying.isContainment =>
          assert(entry.appendAnnotations.isEmpty)
          assert(entry.removeAnnotations.isEmpty)
          assert(entry.updateAnnotations.isEmpty)
          entry.appendElements.foreach {
            case AppendElements(ref: SReference, elems) =>
              val values = elems.asScala.map {
                case o: SObject => EcoreUtil.copy(o.underlying)
              }
              val v = ref.parent.underlying.eGet(ref.underlying).asInstanceOf[EList[EObject]]
              v.addAll(values.asJava)
          }
          entry.removeElements.foreach {
            case RemoveElements(ref2: SReference, elems) =>
              val values = elems.asScala.map {
                case o: SObject =>
                  o.underlying
              }
              val v = ref.parent.underlying.eGet(ref2.underlying).asInstanceOf[EList[EObject]]
              v.removeAll(values.asJava)
          }
        /*entry.insertBefore.foreach {
        case (_,InsertBefore(refr: SObject, elems)) =>
          val values = elems.asScala.map {
            case s: SObject => EcoreUtil.copy(s.underlying)
          }
          ref.parent.underlying.eGet(ref.underlying).asInstanceOf[EList[EObject]]
            .addAll(ref.index,values.asJava)
      }
      entry.insertAfter.foreach {
        case (_,InsertAfter(refr: SObject, elems)) =>
          val values = elems.asScala.map {
            case s: SObject => EcoreUtil.copy(s.underlying)
          }
          ref.parent.underlying.eGet(ref.underlying).asInstanceOf[EList[EObject]]
            .addAll(ref.index + 1,values.asJava)
      }*/
        case (ref: SReference, entry) =>
          val node = resource.getNode(ref.parent.underlying).get.getRelationships(Direction.OUTGOING,Relations.EReference)
              .asScala.find(_.getProperty("name") == ref.underlying.getName).get.getEndNode
          entry.removeElements.foreach {
            case RemoveElements(_, elems) =>
              if (ref.underlying.isOrdered) {
                val list = new NodeList(node)
                val nodes = elems.asScala.map {
                  case ln: SLink =>
                    list.indexOf(list.apply(ln.index))
                  case o: SObject =>
                    resource.getNode(o.underlying)
                }
                list.removeAll(nodes.contains)
              } else {
                val set = new NodeSet(node)
                println("no remove set")
              }
          }
          entry.appendElements.foreach {
            case AppendElements(_, elems) =>
              if (ref.underlying.isOrdered) {
                val list = new NodeList(node)
                /*val nodes = elems.asScala.collect {
                  case ln: SLink =>
                    val lnNode = graph.createNode(Labels.EReference)
                    lnNode.setProperty("uri",ln.label)
                    lnNode
                }
                list.appendAll(nodes)*/
              } else {
                println("no append set")
              }
          }
      }
    }
  }.get
}