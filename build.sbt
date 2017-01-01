enablePlugins(JavaAppPackaging)
enablePlugins(JDKPackagerPlugin)
enablePlugins(UniversalPlugin)
enablePlugins(BuildInfoPlugin)

name := "mscrm-auth"
organization := "crm"
version := "0.1.0"
scalaVersion := "2.11.8"
//scalaVersion := "2.12.1"

scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8", "-Xexperimental")

resolvers += Resolver.url("file://" + Path.userHome.absolutePath + "/.ivy/local")
resolvers += Resolver.sonatypeRepo("releases")
resolvers += "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository"
//resolvers += Resolver.bintrayRepo("scalaz", "releases")
resolvers += Resolver.jcenterRepo


libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" %  "latest.release" % "test"
    ,"org.scala-lang.modules" %% "scala-xml" % "latest.release"
    ,"com.typesafe" % "config" %  "latest.release"
    ,"com.github.scopt" %% "scopt" % "latest.release"
    ,"ch.qos.logback" % "logback-classic" % "latest.release"
    ,"ch.qos.logback" % "logback-core" % "latest.release"
    //,"net.databinder.dispatch" %% "dispatch-core" % "latest.release"
    ,"commons-codec" % "commons-codec" % "latest.release"
    ,"org.scala-lang.modules" %% "scala-async" % "latest.release"
    ,"com.lucidchart" % "xtract_2.11" % "latest.release"
    ,"org.log4s" %% "log4s" % "latest.release"
    ,"com.github.pathikrit" % "better-files_2.11" % "latest.release"
    ,"com.iheart" %% "ficus" % "latest.release"
    ,"org.typelevel" % "cats_2.11" % "latest.version" //"0.7.2" // 0.7.2 for 2.11, if using Xor
    ,"co.fs2" %% "fs2-core" % "latest.release"
    ,"co.fs2" %% "fs2-io" % "latest.release"
    , "co.fs2" %% "fs2-cats" % "latest.release"
    ,"org.apache.commons" % "commons-lang3" % "latest.release"
    ,"org.apache.ddlutils" % "ddlutils" % "latest.release"
    ,"org.asynchttpclient" % "async-http-client" % "latest.version"
    , "io.netty" % "netty-all" % "latest.release"
    , "me.lessis" % "retry_2.11" % "latest.version"
    , "me.lessis" % "odelay-netty_2.11" % "latest.version"
)

libraryDependencies ++= Seq(
  "io.circe" %% "circe-core",
  "io.circe" %% "circe-generic",
  "io.circe" %% "circe-parser",
  "io.circe" %% "circe-optics"
).map(_ % "0.5.4") // 0.5.4 for 2.11

buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion)
buildInfoPackage := "crm"

EclipseKeys.createSrc := EclipseCreateSrc.Default + EclipseCreateSrc.Resource + EclipseCreateSrc.Managed

EclipseKeys.withSource := true

packSettings

packMain := Map("mscrm" -> "crm.program", "diagram" -> "crm.diagram")

fork in run := true

javaOptions in run += "-Xmx8G"

mappings in Universal <+= (packageBin in Compile) map { jar =>
  jar -> ("lib/" + jar.getName)
}

mainClass in Compile := Some("crm.program")

