package org.topleet.apps

import org.topleet.engines.IncrementalParallelEngine
import org.topleet.git.{Resource, SHA}
import org.topleet.{Dynamics, Engine, Leet, Topleets}


object JavaTermsAdded {

  // The relevant library imports.

  import org.topleet.libs.Natives._
  import org.topleet.libs.Gits._

  def main(args: Array[String]): Unit = {
    val out = run(IncrementalParallelEngine.create(), "jwtk/jjwt")

    out.show()
  }

  def run(engine: Engine, address: Address): Leet[SHA, Single[Int]] = {

    val shas: Leet[SHA, Single[SHA]] = git(address)(engine)

    val resources: Leet[SHA, Bag[(Path, Resource)]] = shas.resources()

    val javas: Leet[SHA, Bag[Resource]] = resources
      .collect { case (path, resource) if path.endsWith(".java") => resource }

    val tokens: Leet[SHA, Bag[String]] = javas
      .flatMap(resource => bag(Topleets.tokenize(resource.read())), dynamics = Dynamics.Memoization)

    val diffToParent: Leet[SHA, Bag[String]] = tokens
      .diff(false)
      .added()

    diffToParent.length()
  }

}
