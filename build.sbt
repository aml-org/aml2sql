name := "aml2sql"
organization := "com.github.amlorg"
version := "0.1.0"

scalaVersion := "2.12.0"

resolvers +=
  "MuleSoft releases" at "https://repository-master.mulesoft.org/nexus/content/repositories/releases"
publishTo := Some("GitHub Package Registry" at "https://maven.pkg.github.com/aml-org/aml2sql")
credentials += Credentials("GitHub Package Registry","maven.pkg.github.com",
  sys.env.getOrElse("GITHUB_USER", ""),
  sys.env.getOrElse("GITHUB_TOKEN", "")
)

libraryDependencies += "com.github.amlorg" %% "amf-client" % "3.5.4"
libraryDependencies += "io.circe" %% "circe-parser" % "0.11.1"
libraryDependencies +=  "org.scalatest" %% "scalatest" % "3.0.5" % Test
