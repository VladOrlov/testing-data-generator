import sbt.Keys.libraryDependencies

ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.8"

val log4jVersion = "2.17.2"
val airframeVersion = "22.7.1"
val scoptVersion = "4.0.1"
val avroVersion = "1.11.0"
val jFairyVersion = "0.6.5"

resolvers ++= Seq(
  "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven",
  "Typesafe Simple Repository" at "https://repo.typesafe.com/typesafe/simple/maven-releases",
  "MavenRepository" at "https://mvnrepository.com"
)

lazy val root = (project in file("."))
  .settings(
    name := "testing-data-generator",

    libraryDependencies ++= Seq(
      "org.wvlet.airframe" %% "airframe-http-finagle"% airframeVersion,
      "org.wvlet.airframe" %% "airframe-config" % airframeVersion,
      "org.wvlet.airframe" %% "airframe-ulid" % airframeVersion,
      "org.wvlet.airframe" %% "airframe-log" % airframeVersion,
      "org.apache.avro" % "avro" % avroVersion,

      "com.lihaoyi" %% "requests" % "0.7.1",

      "com.azure" % "azure-messaging-eventhubs" % "5.12.0",
      "com.azure" % "azure-messaging-eventhubs-checkpointstore-blob" % "1.12.2",

      "com.github.scopt" %% "scopt" % scoptVersion,
      "org.yaml" % "snakeyaml" % "1.30",
      "com.devskiller" % "jfairy" % jFairyVersion,
      "com.github.javafaker" % "javafaker" % "1.0.2" exclude("org.yaml", "snakeyaml"),
      "com.vaadin" % "exampledata" % "5.0.1",

      // logging
      "org.apache.logging.log4j" % "log4j-api" % log4jVersion,
      "org.apache.logging.log4j" % "log4j-core" % log4jVersion
    ),

    Compile / mainClass := Some("com.jvo.datagenerator.ServerRunner"),
    assembly / mainClass := Some("com.jvo.datagenerator.ServerRunner")
  )

// META-INF discarding
ThisBuild / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
