package de.dfki.cps.egraph.stools

import de.dfki.cps.egraph.internal.{NodeList, NodeSet}
import de.dfki.cps.egraph.{GraphResource, Labels, Relations}
import de.dfki.cps.secore._
import de.dfki.cps.stools.editscript._
import org.eclipse.emf.common.util.{EList, URI}
import org.eclipse.emf.ecore.EObject
import org.eclipse.emf.ecore.util.EcoreUtil
import org.neo4j.graphdb.{Direction, Node}

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * @author Martin Ring <martin.ring@dfki.de>
  */
object Diff {
  def applyDiff(resource: GraphResource, editScript: SEditScript) = {
    import de.dfki.cps.egraph.internal.Util._
    val graph = resource.root.get.getGraphDatabase
    graph.transaction {
      editScript.entries.foreach {
        case (res: SResource, entry) =>
          val node = resource.root.get.getSingleRelationship(Relations.Contents,Direction.OUTGOING).getEndNode
          assert(entry.appendAnnotations.isEmpty)
          assert(entry.removeAnnotations.isEmpty)
          assert(entry.updateAnnotations.isEmpty)
          entry.removeElements.foreach {
            case RemoveElements(_, elems) =>
              val values = elems.asScala.map {
                case o: SObject => resource.getNode(o.underlying).get
              }.toSet
              val list = new NodeList(node)
              list.removeAll(values.contains)
          }
          entry.insertBefore.foreach {
            case (_, InsertBefore(h: SObject,elems)) =>
              val values = elems.asScala.map {
                case o: SObject => resource.insertEObjectNode(graph)(o.underlying)
              }
              val list = new NodeList(node)
              list.insertAll(list.indexOf(resource.getNode(h.underlying).get), values)
          }
          entry.insertAfter.foreach {
            case (_, InsertAfter(h: SObject,elems)) =>
              val values = elems.asScala.map {
                case o: SObject => resource.insertEObjectNode(graph)(o.underlying)
              }
              val list = new NodeList(node)
              list.insertAll(list.indexOf(resource.getNode(h.underlying).get) + 1, values)
          }
          entry.appendElements.foreach {
            case AppendElements(_, elems) =>
              val values = elems.asScala.map {
                case o: SObject => resource.insertEObjectNode(graph)(o.underlying)
              }
              val list = new NodeList(node)
              list.appendAll(values)
          }
          assert(entry.replaceElements.isEmpty)
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
          assert(entry.insertBefore.isEmpty)
          assert(entry.insertAfter.isEmpty)
          assert(entry.replaceElements.isEmpty)
        case (ref: SReference, entry) =>
          assert(entry.appendAnnotations.isEmpty)
          assert(entry.removeAnnotations.isEmpty)
          assert(entry.updateAnnotations.isEmpty)
          assert(entry.replaceElements.isEmpty)
          if (ref.underlying.isContainment && ref.underlying.isMany) {
            val node = {
              val objNode = resource.getNode(ref.parent.underlying).get
              objNode.getRelationships(Direction.OUTGOING, Relations.EReference)
                .asScala.find(_.getProperty("name") == ref.underlying.getName)
                .map(_.getEndNode).getOrElse {
                val root = if (ref.underlying.isOrdered) NodeList.empty(graph).root else NodeSet.empty(graph).root
                val rel = objNode.createRelationshipTo(root,Relations.EReference)
                rel.setProperty("name",ref.underlying.getName)
                root
              }
            }
            entry.removeElements.foreach {
              case RemoveElements(ref2: SReference, elems) =>
                val values = elems.asScala.map {
                  case o: SObject => resource.getNode(o.underlying).get
                }.toSet
                if (ref.underlying.isOrdered) {
                  val list = new NodeList(node)
                  list.removeAll(values.contains)
                } else {
                  val set = new NodeSet(node,ref.underlying.isUnique)
                  assert(values.forall(set.remove))
                }
            }
            entry.insertBefore.foreach {
              case (_, InsertBefore(h: SObject,elems)) =>
                val values = elems.asScala.map {
                  case o: SObject => resource.insertEObjectNode(graph)(o.underlying)
                }
                assert (ref.underlying.isOrdered)
                val list = new NodeList(node)
                list.insertAll(list.indexOf(resource.getNode(h.underlying).get), values)
            }
            entry.insertAfter.foreach {
              case (_, InsertAfter(h: SObject,elems)) =>
                val values = elems.asScala.map {
                  case o: SObject => resource.insertEObjectNode(graph)(o.underlying)
                }
                assert (ref.underlying.isOrdered)
                val list = new NodeList(node)
                list.insertAll(list.indexOf(resource.getNode(h.underlying).get) + 1, values)
            }
            entry.appendElements.foreach {
              case AppendElements(ref: SReference, elems) =>
                val values = elems.asScala.map {
                  case o: SObject => resource.insertEObjectNode(graph)(o.underlying)
                }
                if (ref.underlying.isOrdered) {
                  val list = new NodeList(node)
                  list.appendAll(values)
                } else {
                  val set = new NodeSet(node,ref.underlying.isUnique)
                  set ++= values
                }
            }
          } else if (ref.underlying.isContainment) {
            val node = resource.getNode(ref.parent.underlying).get
            assert(entry.insertAfter.isEmpty)
            assert(entry.insertBefore.isEmpty)
            entry.removeElements.foreach {
              case RemoveElements(_,elems) =>
                assert(elems.size() == 1)
                ref.underlying
                val rel = node.getRelationships(Direction.OUTGOING,Relations.EReference)
                    .asScala.find(_.getProperty("name") == ref.underlying.getName).get
                val refNode = rel.getEndNode
                rel.delete()
                deleteTransitiveOut(
                  refNode,
                  Relations.Contents,Relations.EReference,
                  de.dfki.cps.egraph.internal.Relations.MEMBER,
                  de.dfki.cps.egraph.internal.Relations.NEXT_SIBLING
                )
            }
            entry.appendElements.foreach {
              case AppendElements(_,elems) =>
                assert(elems.size() == 1)
                val elem = elems.get(0).asInstanceOf[SObject]
                val refNode = resource.insertEObjectNode(graph)(elem.underlying)
                val rel = node.createRelationshipTo(refNode,Relations.EReference)
                rel.setProperty("name", ref.underlying.getName)
            }
          }
          else if (ref.underlying.isMany) {
            val node = {
              val objNode = resource.getNode(ref.parent.underlying).get
              objNode.getRelationships(Direction.OUTGOING, Relations.EReference)
                .asScala.find(_.getProperty("name") == ref.underlying.getName)
                .map(_.getEndNode).getOrElse {
                val root = if (ref.underlying.isOrdered) NodeList.empty(graph).root else NodeSet.empty(graph).root
                val rel = objNode.createRelationshipTo(root,Relations.EReference)
                rel.setProperty("name",ref.underlying.getName)
                root
              }
            }
            entry.removeElements.foreach {
              case RemoveElements(_, elems) =>
                if (ref.underlying.isOrdered) {
                  assert(node.hasLabel(de.dfki.cps.egraph.internal.Labels.LIST))
                  val list = new NodeList(node)
                  val nodes = elems.asScala.map {
                    case ln: SLink =>
                      list.apply(ln.index)
                  }
                  nodes.foreach(n => list.remove(list.indexOf(n)))
                } else {
                  assert(node.hasLabel(de.dfki.cps.egraph.internal.Labels.SET))
                  val set = new NodeSet(node,ref.underlying.isUnique)
                  elems.asScala.foreach {
                    case ln: SLink =>
                      assert(set.remove(set.find(_.getProperty("uri") == ln.label).get))
                  }
                }
            }
            entry.appendElements.foreach {
              case AppendElements(_, elems) =>
                val nodes = elems.asScala.collect {
                  case ln: SLink =>
                    val lnNode = graph.createNode(Labels.EReference)
                    lnNode.setProperty("uri", ln.label)
                    lnNode
                }
                if (ref.underlying.isOrdered) {
                  val list = new NodeList(node)
                  list.appendAll(nodes)
                } else {
                  val set = new NodeSet(node, ref.underlying.isUnique)
                  set ++= nodes
                }
            }
            assert(entry.insertAfter.isEmpty)
            assert(entry.insertBefore.isEmpty)
          } else {
            assert(false)
          }
      }
    }.get
  }
}