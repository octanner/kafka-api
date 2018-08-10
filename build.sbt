import scalariform.formatter.preferences._
import com.typesafe.sbt.SbtScalariform
import com.typesafe.sbt.SbtScalariform.ScalariformKeys

name := """kafka-api"""

version := "1.0-SNAPSHOT"

lazy val root = (project in file(".")).enablePlugins(PlayScala)

scalaVersion := "2.12.6"

credentials ++= {
  (sys.env.get("OCT_VAULT_SHARED_READ_ARTIFACTORY_USERNAME"), sys.env.get("OCT_VAULT_SHARED_READ_ARTIFACTORY_PASSWORD")) match {
    case (Some(user), Some(token)) => Seq(Credentials("Artifactory Realm", "artifactory.octanner.net",user,token))
    case _ => Seq[Credentials]()
  }
}

resolvers ++= Seq(
  "OCTanner releases" at "https://artifactory.octanner.net/releases/",
  "OCTanner snapshots" at "https://artifactory.octanner.net/snapshots/",
  "OCTanner plugins releases" at "https://artifactory.octanner.net/plugins-releases/",
  "OCTanner plugins snapshots" at "https://artifactory.octanner.net/plugins-snapshots/",
  Resolver.mavenLocal,
  Resolver.jcenterRepo
)

coverageMinimum := 0
coverageFailOnMinimum := false

libraryDependencies += jdbc
libraryDependencies += ehcache
libraryDependencies += ws
libraryDependencies += guice
libraryDependencies += evolutions
libraryDependencies += "com.typesafe.play" %% "anorm" % "2.5.3"
libraryDependencies += "org.postgresql" % "postgresql" % "9.4.1208"
libraryDependencies += "org.scalatestplus.play" %% "scalatestplus-play" % "3.0.+" % Test
libraryDependencies += "com.octanner.platform" %% "service-auth-play" % "1.2.+"
libraryDependencies += "com.octanner" %% "ws-tracer-client-play" % "0.0.1"
libraryDependencies += "com.octanner.platform" %% "service-auth-play-test" % "1.2.+" % Test
libraryDependencies += "com.octanner.platform" %% "platform-test-tools" % "1.1.1" % Test
libraryDependencies += "org.apache.kafka" %% "kafka" % "2.0.0"
libraryDependencies += "net.manub" %% "scalatest-embedded-kafka" % "1.0.0" % "test"
libraryDependencies += "io.swagger" %% "swagger-play2" % "1.6.+"
coverageExcludedPackages := "<empty>;Reverse.*;views.*;router.*;database.*"

javaOptions in Test ++= Seq("-Dconfig.file=conf/application.test.conf", "-Dplay.evolutions.db.default.autoApply=true")

val preferences =
  ScalariformKeys.preferences := ScalariformKeys.preferences.value
    .setPreference(AlignSingleLineCaseStatements, true)
    .setPreference(AlignParameters, true)
    .setPreference(DoubleIndentConstructorArguments, true)
    .setPreference(DanglingCloseParenthesis, Preserve)

SbtScalariform.scalariformSettings ++ Seq(preferences)
