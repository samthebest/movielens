
Defaults.itSettings

lazy val `it-config-sbt-project` = project.in(file(".")).configs(IntegrationTest.extend(Test))

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.scalacheck" %% "scalacheck" % "1.12.1" % "it,test" withSources() withJavadoc(),
  "org.specs2" %% "specs2-core" % "2.4.15" % "it,test" withSources() withJavadoc(),
  "org.specs2" %% "specs2-scalacheck" % "2.4.15" % "it,test" withSources() withJavadoc(),
  //
  "org.apache.spark" %% "spark-core" % "2.3.2" withSources(),
  "org.apache.spark" %% "spark-sql" % "2.3.2" withSources()
)

javaOptions ++= Seq("-target", "1.8", "-source", "1.8")

name := "movielens"

parallelExecution in Test := false

version := "1"
