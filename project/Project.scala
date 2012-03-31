import sbt._
import Keys._
import sbtprotobuf.{ProtobufPlugin=>PB}

object mryBuild extends Build {
	var commonResolvers = Seq(
		// local snapshot support
		ScalaToolsSnapshots,

		// common deps
		"Maven.org" at "http://repo1.maven.org/maven2",
		"Sun Maven2 Repo" at "http://download.java.net/maven/2",
		"Scala-Tools" at "http://scala-tools.org/repo-releases/",
		"Sun GF Maven2 Repo" at "http://download.java.net/maven/glassfish",
		"Oracle Maven2 Repo" at "http://download.oracle.com/maven",
		"Sonatype" at "http://oss.sonatype.org/content/repositories/release"
	)

	var commonDeps = Seq (
		"com.appaquet" %% "nrv-core" % "0.1-SNAPSHOT",
		"c3p0" % "c3p0" % "0.9.1.2",
		"mysql" % "mysql-connector-java" % "5.1.6",
		"org.scalatest" %% "scalatest" % "1.7.1" % "test",
		"junit" % "junit" % "4.10" % "test"
	)

	lazy val root = Project(
		id = "mry",
		base = file(".")
	) aggregate(core)

	lazy val core = Project(	
		id = "mry-core",
		base = file("mry-core"),
		settings = Defaults.defaultSettings ++ PB.protobufSettings ++ Seq(
			libraryDependencies ++= commonDeps, 
			resolvers ++= commonResolvers

		) ++ Seq(retrieveManaged := true)
	)
}

