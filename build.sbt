name := "topleet"
organization := "org.topleet"

version := "1.2.0"

val buildSettings = Defaults.coreDefaultSettings ++ Seq(
  scalaVersion := "2.12.10"
)

lazy val core = project
  .settings(
    buildSettings,
    libraryDependencies ++= Seq(
      "com.google.guava" % "guava" % "28.2-jre",
      "commons-io" % "commons-io" % "2.6",
      "org.scalatest" %% "scalatest" % "3.2.0-M2" % Test,
      "org.scalacheck" %% "scalacheck" % "1.14.0" % Test
    )
  )

lazy val git = project
  .settings(
    buildSettings,
    libraryDependencies ++= Seq(
      "org.eclipse.jgit" % "org.eclipse.jgit" % "5.6.0.201912101111-r",
      "org.apache.commons" % "commons-text" % "1.8"
    )
  ).dependsOn(core)

lazy val spark = project
  .settings(
    buildSettings,
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "2.4.4"
    )
  ).dependsOn(core)

lazy val apps = project
  .settings(
    buildSettings,
    libraryDependencies ++= Seq(
      "org.eclipse.jdt" % "org.eclipse.jdt.core" % "3.20.0"
    )
  ).dependsOn(core, git, spark)

lazy val eval = project
  .settings(
    buildSettings,
    libraryDependencies ++= Seq(
    )
  ).dependsOn(core, git, spark, apps)