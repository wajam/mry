import sbt._
import Keys._
import com.typesafe.startscript.StartScriptPlugin

object MryBuild extends Build {
  var commonResolvers = Seq(
    // local snapshot support
    ScalaToolsSnapshots,

    // common deps
    "Maven.org" at "http://repo1.maven.org/maven2",
    "Sun Maven2 Repo" at "http://download.java.net/maven/2",
    "Scala-Tools" at "http://scala-tools.org/repo-releases/",
    "Sun GF Maven2 Repo" at "http://download.java.net/maven/glassfish",
    "Oracle Maven2 Repo" at "http://download.oracle.com/maven",
    "Sonatype" at "http://oss.sonatype.org/content/repositories/release",
    "Cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
    "Wajam" at "http://ci1.is.wajam/"
  )

  var commonDeps = Seq(
    "com.wajam" %% "nrv-core" % "0.1-SNAPSHOT",
    "com.wajam" %% "spnl-core" % "0.1-SNAPSHOT",
    "com.wajam" %% "scn-core" % "0.1-SNAPSHOT",
    "com.google.protobuf" % "protobuf-java" % "2.4.1",
    "c3p0" % "c3p0" % "0.9.1.2",
    "org.rogach" %% "scallop" % "0.6.0",
    "mysql" % "mysql-connector-java" % "5.1.6",
    "org.scalatest" %% "scalatest" % "1.7.1" % "test,it",
    "junit" % "junit" % "4.10" % "test,it",
    "org.mockito" % "mockito-core" % "1.9.0" % "test,it"
  )

  val defaultSettings = Defaults.defaultSettings ++ Defaults.itSettings ++ Seq(
    libraryDependencies ++= commonDeps,
    resolvers ++= commonResolvers,
    retrieveManaged := true,
    publishMavenStyle := true,
    organization := "com.wajam",
    version := "0.1-SNAPSHOT"
  )

  lazy val root = Project("mry", file("."))
    .configs(IntegrationTest)
    .settings(defaultSettings: _*)
    .settings(testOptions in IntegrationTest := Seq(Tests.Filter(s => s.contains("Test"))))
    .settings(StartScriptPlugin.startScriptForClassesSettings: _*)
    .aggregate(core)

  import sbtprotobuf.{ProtobufPlugin => PB}

  val protobufSettings = PB.protobufSettings ++ Seq(
    javaSource in PB.protobufConfig <<= (sourceDirectory in Compile)(_ / "java")
  )

  lazy val core = Project("mry-core", file("mry-core"))
    .configs(IntegrationTest)
    .settings(defaultSettings: _*)
    // See http://code.google.com/p/protobuf/issues/detail?id=368
    // We don't publish docs because it doesn't work with java generated by protobuf
    .settings(publishArtifact in packageDoc := false)
    .settings(testOptions in IntegrationTest := Seq(Tests.Filter(s => s.contains("Test"))))
    .settings(StartScriptPlugin.startScriptForClassesSettings: _*)
    .settings(parallelExecution in IntegrationTest := false)
    .settings(protobufSettings: _*)
}

