enablePlugins(JavaAppPackaging)
enablePlugins(JDKPackagerPlugin)
enablePlugins(UniversalPlugin)

name := "mscrm-auth"
organization := "crm"
version := "1.0"
scalaVersion := "2.11.8"

scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8")

resolvers += Resolver.url("file://" + Path.userHome.absolutePath + "/.ivy/local")
resolvers += Resolver.sonatypeRepo("releases")
resolvers += "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository"
resolvers += Resolver.bintrayRepo("scalaz", "releases")
resolvers += Resolver.jcenterRepo


libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" %  "latest.release" % "test"
    ,"org.scala-lang.modules" %% "scala-xml" % "latest.release"
    ,"com.typesafe" % "config" %  "latest.release"
    ,"com.github.scopt" %% "scopt" % "latest.release"
    ,"ch.qos.logback" % "logback-classic" % "latest.release"
    ,"ch.qos.logback" % "logback-core" % "latest.release"
    ,"net.databinder.dispatch" %% "dispatch-core" % "latest.release"
    ,"commons-codec" % "commons-codec" % "latest.release"
    ,"org.scala-lang.modules" %% "scala-async" % "latest.release"
    ,"com.lucidchart" %% "xtract" % "latest.release"
    ,"org.log4s" %% "log4s" % "latest.release"
    ,"com.github.pathikrit" %% "better-files" % "latest.release"
    ,"com.iheart" %% "ficus" % "latest.release"
    ,"org.typelevel" %% "cats" % "latest.release"
    ,"co.fs2" %% "fs2-core" % "latest.release"
    ,"co.fs2" %% "fs2-io" % "latest.release"
    ,"org.apache.commons" % "commons-lang3" % "latest.release"
)

libraryDependencies ++= Seq(
  "io.circe" %% "circe-core",
  "io.circe" %% "circe-generic",
  "io.circe" %% "circe-parser"
).map(_ % "latest.version")

EclipseKeys.createSrc := EclipseCreateSrc.Default + EclipseCreateSrc.Resource

EclipseKeys.withSource := true

packSettings

packMain := Map("mscrm" -> "crm.program", "diagram" -> "crm.diagram")

fork in run := true

javaOptions in run += "-Xmx8G"

mappings in Universal <+= (packageBin in Compile) map { jar =>
  jar -> ("lib/" + jar.getName)
}



