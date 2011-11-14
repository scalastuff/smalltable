
organization := "org.scalastuff"

name := "smalltable"

version := "0.1-SNAPSHOT"

scalaVersion := "2.9.0-1"

libraryDependencies ++= Seq (
        "org.scalastuff" % "scalabeans_2.9.0" % "0.2", 
        "org.scalaz" %% "scalaz-core" % "6.0.1", 
        "me.prettyprint" % "hector-core" % "1.0-1",
        "com.dyuproject.protostuff" % "protostuff-api" % "1.0.3",
        "com.dyuproject.protostuff" % "protostuff-core" % "1.0.3",
        "junit" % "junit" % "4.8" % "test")

resolvers ++= Seq(
        "scala tools" at "http://scala-tools.org/repo-releases/",
        "repo.novus snaps" at "http://repo.novus.com/snapshots/"
)

