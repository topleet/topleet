
val buildSettings = Defaults.coreDefaultSettings ++ Seq(
  scalaVersion := "2.12.10",
  organization := "org.topleet",
  version := "0.1.0", //-SNAPSHOT
  developers := List(
    Developer(
      id = "johanneshaertel",
      name = "Johannes",
      email = "johanneshaertel@uni-koblenz.de",
      url = url("http://topleet.org")
    )
  ),
  homepage := Some(url("https://github.com/topleet/topleet")),
  scmInfo := Some(ScmInfo(url("https://github.com/topleet/topleet"), "git@github.com:topleet/topleet.git")),
  licenses += ("GPL-3.0", url("https://www.gnu.org/licenses/gpl-3.0.en.html")),
  credentials += Credentials(Path.userHome / ".sbt" / "sonatype_credentials"),
  sonatypeProfileName := "org.topleet",
  publishTo := Some(
    if (isSnapshot.value)
      Opts.resolver.sonatypeSnapshots
    else
      Opts.resolver.sonatypeStaging
  )
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
      "org.eclipse.jgit" % "org.eclipse.jgit" % "5.9.0.202009080501-r",
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