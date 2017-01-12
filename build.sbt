organization := "de.dfki.cps"
name := "egraph"
scalaVersion := "2.11.8"
version := "0.2.0"
licenses += ("MIT", url("http://opensource.org/licenses/MIT"))
bintrayOrganization := Some("dfki-cps")

scalacOptions := Seq("-deprecation")

crossScalaVersions := Seq("2.11.8","2.12.1")

libraryDependencies += "de.dfki.cps" % "specific-dependencies" % "4.6.3"
libraryDependencies += "org.neo4j" % "neo4j" % "3.1.0"