package de.dfki.cps.egraph

import java.io.{File, InputStream, OutputStream}
import java.net.URL
import java.util

import org.eclipse.emf.common.util.URI
import org.eclipse.emf.ecore.resource.Resource.Factory
import org.eclipse.emf.ecore.resource._
import org.eclipse.emf.ecore.resource.impl.ExtensibleURIConverterImpl
import org.neo4j.graphdb.{GraphDatabaseService, Transaction}
import org.neo4j.graphdb.factory.GraphDatabaseFactory

import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

class EGraphStoreInput(val store: EGraphStore) extends InputStream {
  def read(): Int =
    throw new UnsupportedOperationException("cannot read from graph input stream.")
}

class EGraphStoreOutput(val store: EGraphStore) extends OutputStream {
  def write(b: Int): Unit =
    throw new UnsupportedOperationException("cannot write to graph output stream.")
}

class EGraphStore(dir: File, settings: Option[URL] = None) {
  assert(dir.isDirectory, s"$dir is not a directory")

  lazy val graphDb: GraphDatabaseService = {
    val builder = new GraphDatabaseFactory()
      .newEmbeddedDatabaseBuilder(dir)
    val configuredBuilder =
      settings.fold(builder)(builder.loadPropertiesFromURL)
    val db = configuredBuilder.newGraphDatabase()
    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = db.shutdown()
    })
    db
  }

  def dispose(): Unit = graphDb.shutdown()

  def attach(resourceSet: ResourceSet): Unit = {
    val emptyInputStream = new EGraphStoreInput(this)
    val emptyOutputStream = new EGraphStoreOutput(this)
    resourceSet.setURIConverter(new ExtensibleURIConverterImpl() {
      override def createInputStream(uri: URI, options: util.Map[_, _]): InputStream =
        if (uri.scheme() == "graph") {
          emptyInputStream
        } else super.createInputStream(uri, options)

      override def createOutputStream(uri: URI, options: util.Map[_,_]): OutputStream =
        if (uri.scheme() == "graph") {
          emptyOutputStream
        } else super.createOutputStream(uri, options)
    })
    resourceSet.getResourceFactoryRegistry.getProtocolToFactoryMap.put("graph",
      new Factory {
        def createResource(uri: URI) = new GraphResource(uri)
      }
    )
  }
}
