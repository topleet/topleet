package org.topleet.libs

import java.awt.Desktop
import java.io.File
import java.net.URI
import java.util.Date

import com.google.common.base.Charsets
import com.google.common.io.Files
import org.apache.commons.text.StringEscapeUtils
import org.topleet
import org.topleet.git.{Gitobject, Resource, SHA}
import org.topleet.{Dynamics, Engine, Leet, Topleets}

import scala.reflect.{ClassTag => CT}


/**
 * The Topleet library providing Git specific functionality:
 * {{{
 *     val shas: Leet[SHA, Single[SHA]] = git("jwtk/jjwt")
 *     shas.show()
 *
 *     val resources: Leet[SHA, Bag[(Path, Resource)]] = shas.resources()
 *
 * }}}
 */
object Gits {

  import Natives._

  type Address = String
  type Path = String


  def git(address: Set[Address])(implicit engine: Engine): Leet[SHA, Single[SHA]] =
    address.map(x => git(x)).reduce(_ tadd _)

  // TODO: Remove empty SHA?
  def git(address: Address)(implicit engine: Engine): Leet[SHA, Single[SHA]] = {

    def nodes(): Set[SHA] = {
      val go = Gitobject(address)
      val commits = go.commits()
      Set(SHA(address, None)) ++ commits.map(x => SHA(address, Some(x)))
    }

    def inEdges(n: SHA): Set[SHA] = {
      val go = Gitobject(address)

      n match {
        case SHA(_, None) => Set[SHA]()
        case SHA(_, Some(x)) if go.parentsCount(x) == 0 => Set(SHA(address, None))
        case SHA(_, Some(x)) => go.parents(x).map(y => SHA(address, Some(y))).toSet
      }
    }

    def values(n: SHA): Single[SHA] = single(n)

    engine.create(() => nodes(), inEdges, values)
  }

  implicit class GitProvide[N: CT](@transient l: Leet[N, Single[SHA]]) extends Serializable {

    type Name = String
    type Email = String

    def message(): Leet[N, Single[String]] = {
      l.topology().collect { case SHA(address, Some(sha)) =>
        Gitobject(address).comment(sha)
      }
    }

    def author(): Leet[N, Single[(Name, Email)]] = {
      l.topology().collect { case SHA(address, Some(sha)) =>
        (Gitobject(address).authorName(sha), Gitobject(address).authorMail(sha))
      }
    }

    def time(): Leet[N, Single[Int]] = {
      l.topology().collect { case SHA(address, Some(sha)) =>
        Gitobject(address).time(sha)
      }
    }

    def date(): Leet[N, Single[Date]] = time().map { time => new Date(time * 1000l) }

    def resources(): Leet[N, Bag[(Path, Resource)]] = {
      l.groupBy(_ => null.asInstanceOf[Unit])
        .tmapHom({ case (_, shas) =>
          def lasts = shas.toSeq.flatMap {
            case (sha, count) if count < 0 => Seq.fill(-count)(sha)
            case _ => Seq[SHA]()
          }

          def nexts = shas.toSeq.flatMap {
            case (sha, count) if count > 0 => Seq.fill(count)(sha)
            case _ => Seq[SHA]()
          }

          // Pairs that can be diffed.
          val pairs: Seq[(SHA, SHA)] = lasts.zipAll(nexts, SHA("", None), SHA("", None))

          val diffs = pairs.flatMap {
            case (SHA(address, Some(sha1)), SHA(_, Some(sha2))) => Gitobject(address).diff(Some(sha1), Some(sha2)).map(x => (address, x))
            case (SHA(address, Some(sha1)), SHA(_, None)) => Gitobject(address).diff(Some(sha1), None).map(x => (address, x))
            case (SHA(_, None), SHA(address, Some(sha2))) => Gitobject(address).diff(None, Some(sha2)).map(x => (address, x))
            case (SHA(_, None), SHA(_, None)) => Seq()
          }

          val abl = aMap[(Path, Resource), Int]

          val adds = bag(diffs.collect { case (address, (path, _, Some(res))) => (path, topleet.git.Resource(address, res)) })
          val removes = bag(diffs.collect { case (address, (path, Some(res), _)) => (path, topleet.git.Resource(address, res)) })

          abl.op(abl.inv(removes), adds)
        }, dynamics = Dynamics.Collapse)
    }
  }

  implicit class LibraryGitplotting[K: CT, V: CT](@transient l: Leet[SHA, Map[K, V]]) extends Serializable {

    def show(contract: Boolean = true, removeBypass: Boolean = true, showEdgeLabels: Boolean = false, horizontal: Boolean = false, file: File = new File("temp/viewer"), open: Boolean = true): Unit = {
      val abl = aMap[K, V](l.g())
      val ns = l.nodes().toSet
      val es = l.edges().toSet
      val index = l.index().toMap
      val changes = es.map { case (n1, n2) => (n1, n2) -> abl.diff(index(n1), index(n2)) }.toMap
      //val bypass = if (removeBypass) Topology.bypass(ns, es).toSet else Set[(SHA, SHA)]()

      val (ns2, es2, mapping2) = if (contract) Topleets.accontract(ns, es, changes.filter(_._2 == abl.zero()).keySet, removeBypass) else (ns, es, ns.map(n => n -> n).toMap)
      val imapping = Topleets.groupByValue(mapping2.toSeq)

      val ges = es2.zipWithIndex
      val gns = ns2.zipWithIndex.toMap

      def attributes(x: Map[String, String]) = "[" + x.map { case (k, v) => k + "=\"" + StringEscapeUtils.escapeEcmaScript(v) + "\"" }.reduceOption(_ + ", " + _).getOrElse("") + "]"

      def label(l: Any): String = l match {
        case m: Map[K, V] if m.size > 20 => label(m.take(20)) + "\n . . . "
        case m: Map[K, Int] if scala.reflect.classTag[V] == scala.reflect.classTag[Int] => m.map {
          case (k, v) if v == 1 => k.toString
          case (k, v) if v > 1 => k.toString + " (" + v + " elements)"
          case (k, v) if v < 1 => k.toString + " (" + v + " elements)"
        }.reduceOption(_ + "\n" + _).getOrElse(" ")
        case m: Map[K, V] => m.map {
          case (k, v) => k.toString + " -> " + v.toString
        }.reduceOption(_ + "\n" + _).getOrElse(" ")
        case s if s.toString.length <= 300 => s.toString
        case s => s.toString.take(300) + " . . ."
      }

      val edgesDot = ges.map { case ((n1, n2), id) => "nd" + gns(n1) + " -> " + "nd" + gns(n2) + attributes(Map("id" -> ("e" + id)) ++ Map("label" -> (if (showEdgeLabels) label(changes(n1, n2)) else ""))) + ";" }.reduceOption(_ + " " + _).getOrElse("")
      val vertexDot = gns.map { case (n, id) => "nd" + id + attributes(Map("id" -> ("n" + id)) ++ Map("label" -> label(index(n)), "shape" -> (if (label(index(n)).contains("\n")) "box" else "ellipse"))) + ";" }.reduceOption(_ + " " + _).getOrElse("")

      def kv(key: String, value: String): String = "\"" + key + "\":\"" + value + "\""

      val json = (ges.map { case ((sha1, sha2), eid) =>
        val annotations = "{}" // "{" + kv("bypass", bypass((sha1, sha2)).toString) + "}"
        val hover = ""
        "\"" + "e" + eid + "\":{\"hover\":[" + hover + "],\"svg\":" + annotations + "}"
      } ++ gns.map { case (nodeSha, nid) =>
        val annotations = "{" + kv("fstLine", "true") + "}"

        val hover = imapping(nodeSha)
          .collect { case SHA(address, Some(sha)) => (address, sha) }
          .sortBy { case (address, sha) => Gitobject(address).time(sha) }
          .map { case (address, sha) =>
            val name = StringEscapeUtils.escapeEcmaScript(Gitobject(address).authorName(sha))
            val mail = StringEscapeUtils.escapeEcmaScript(Gitobject(address).authorMail(sha))
            val time = StringEscapeUtils.escapeEcmaScript(Gitobject(address).time(sha).toString)
            val commit = s"<a href='https://github.com/$address/commit/$sha'>$sha</a>"
            val owner = nodeSha.sha.contains(sha).toString

            "{" + kv("owner", owner) + "," + kv("name", name) + "," + kv("mail", mail) + "," + kv("time", time) + "," + kv("commit", commit) + "}"
          }.reduceOption(_ + "," + _).getOrElse("")

        "\"" + "n" + nid + "\":{\"hover\":[" + hover + "],\"svg\":" + annotations + "}"
      }).reduceOption(_ + "," + _).getOrElse("")

      val options = if (horizontal) "rankdir=\"LR\";" else ""


      val templete = Files.asCharSource(new File("git/src/main/resources/viewer/viewer.html"), Charsets.UTF_8).read()
      val instance = templete
        .replace("digraph R {}", "digraph R {" + StringEscapeUtils.escapeEcmaScript(options + vertexDot + edgesDot).replace("\\\\r\\\\n", "\\\\l") + "}")
        .replace("jsondata = {}", "jsondata = {" + json + "}")

      file.mkdirs()
      Files.asCharSink(new File(file, "viewer.html"), Charsets.UTF_8).write(instance)

      Files.copy(new File("git/src/main/resources/viewer/d3.v4.min.js"), new File(file, "d3.v4.min.js"))
      Files.copy(new File("git/src/main/resources/viewer/d3-graphviz.min.js"), new File(file, "d3-graphviz.min.js"))
      Files.copy(new File("git/src/main/resources/viewer/viz.js"), new File(file, "viz.js"))

      if (open)
        if (System.getProperty("os.name") == "Windows 10" || System.getProperty("os.name").indexOf("win") >= 0)
          Runtime.getRuntime.exec("rundll32 url.dll,FileProtocolHandler " + new File(file, "viewer.html").getAbsolutePath)
        else
          Desktop.getDesktop.browse(new URI("file://" + new File(file, "viewer.html").getAbsolutePath))

    }

  }


}
