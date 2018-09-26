name := "SparkConnector"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.1.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.1.1"

esolvers += "Akka Repository" at "http://repo.akka.io/releases/"

resolvers += Resolver.url("artifactory", url("http://scalasbt.artifactoryonline.com/scalasbt/sbt-plugin-releases"))(Resolver.ivyStylePatterns)

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.11.2")





