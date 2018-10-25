name := "Spark-Exercise"

version := "1.0"

scalaVersion := "2.11.8"

scalacOptions ++= Seq("-feature")

publishArtifact in Test := true

resolvers += "MarkLogic Releases" at "http://developer.marklogic.com/maven2"

libraryDependencies += "com.marklogic" % "java-client-api" % "4.0.0-EA3"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.1" exclude("javax.servlet", "servlet-api")

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.1" exclude("javax.servlet", "servlet-api")

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.3.1" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.3.1" % "runtime" exclude("javax.servlet", "servlet-api")

libraryDependencies += "com.marklogic" % "marklogic-mapreduce2" % "2.2.6" exclude("javax.servlet", "servlet-api")

libraryDependencies += "com.google.code.gson" % "gson" % "2.8.5"