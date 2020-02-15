package org.topleet

import java.util.regex.Pattern

import org.topleet.engines.IncrementalParallelEngine
import org.topleet.groups.{GMap, Group}

import scala.collection.mutable

/**
 * Utility methods.
 */
object Topleets {

  def groupByKey[K, V](x: Seq[(K, V)]): Map[K, Seq[V]] = x.groupBy(_._1).map { case (k, vs) => (k, vs.map(_._2)) }

  def groupByValue[K, V](x: Seq[(K, V)]): Map[V, Seq[K]] = groupByKey(x.map(_.swap))

  def reduceByKey[K, V](tuples: Seq[(K, V)])(f: (V, V) => V): Map[K, V] = groupByKey(tuples).map { case (k, seq) => (k, seq.reduce(f)) }

  def replace(map: Map[String, String]): String => String = x => map.foldRight(x)((l, r) => r.replaceAll(l._1, l._2))

  def keepLetters(x: String): String = replace(Map("[^a-zA-Z]" -> " "))(x)

  def splitCamelCase(x: String): String = insert("[A-Z][A-Z][a-z]", " ", 1)(insert("[a-z][A-Z]", " ", 1)(x))

  def toLower(x: String): String = x.toLowerCase

  def tokenize(x: String): Seq[String] = x.split(" ").filter { x => !("".equals(x) || " ".equals(x)) }

  def insert(regex: String, content: String, offset: Integer): String => String = x => {
    val matcher = Pattern.compile(regex).matcher(x)
    var current = x
    var number = 0
    while (matcher.find()) {
      def position = matcher.start() + offset + number

      current = current.substring(0, position) + content + current.substring(position)
      number = number + 1
    }
    current
  }

  /**
   * Creates a topological sorting with the connected components chained after each other.
   */
  def tsCCSeq[N](edges: Set[(N, N)], nodes: Set[N]): Seq[N] = {
    val ccmapping = ccs2(edges, nodes)
    val ccs = ccmapping.values.toSet
    val ccnodes = groupByValue(ccmapping.toSeq)
    val ccedges = edges.groupBy { case (n, _) => ccmapping(n) }
    // There might be single node components.
    ccs.toSeq.flatMap { cc => ts(ccnodes(cc).toSet, ccedges.getOrElse(cc, Set())) }
  }

  def cut[A](xs: Seq[A], n: Int): Iterator[Seq[A]] = {
    val (quot, rem) = (xs.size / n, xs.size % n)
    val (smaller, bigger) = xs.splitAt(xs.size - rem * (quot + 1))
    smaller.grouped(quot) ++ bigger.grouped(quot + 1)
  }

  def incoming[N](ns: Set[N], es: Set[(N, N)]): Map[N, Set[N]] = {
    val in = es.toSeq.groupBy(_._2).mapValues(_.map(_._1)).withDefaultValue(Seq[N]())
    ns.map(n => (n, in.getOrElse(n, Seq()).toSet)).toMap
  }

  def outgoing[N](ns: Set[N], es: Set[(N, N)]): Map[N, Set[N]] = {
    val out = es.toSeq.groupBy(_._1).mapValues(_.map(_._2)).withDefaultValue(Seq[N]())
    ns.map(n => (n, out.getOrElse(n, Seq()).toSet)).toMap
  }

  def ts[N](ns: Set[N], es: Set[(N, N)]): Iterator[N] = {
    val ins = incoming(ns, es)
    val outs = outgoing(ns, es)

    val border = mutable.Queue[N]()
    val visited = mutable.Set[N]()
    ns.filter(x => ins(x).isEmpty).foreach(border.enqueue(_))

    Iterator.fill(ns.size) {
      // Take a random node from the border
      val current = border.dequeue()
      visited.add(current)
      // Add now available elements to the border.
      for (o <- outs(current) if ins(o).forall(visited))
        border.enqueue(o)

      current
    }
  }

  def index2[N, V](ns: Set[N], es: Set[(N, N)], cp: Set[N],
                   checkps: Map[N, V], changes: Map[(N, N), V],
                   group: Group[V]): Iterator[(N, V)] = new Iterator[(N, V)] {

    val visited: mutable.Set[N] = mutable.Set[N]()
    val border: mutable.Queue[(N, V)] = mutable.Queue[(N, V)]()

    val edges: Map[N, Set[N]] = (es ++ es.map(_.swap))
      .groupBy(_._1)
      .mapValues(_.map(_._2)).withDefaultValue(Set())

    // Add checkpoints
    for (n <- cp) {
      visited.add(n)
      border.enqueue((n, checkps.getOrElse(n, group.zero())))
    }

    override def hasNext: Boolean = border.nonEmpty

    override def next(): (N, V) = {
      val (n, index) = border.dequeue()
      val updates = edges(n).filter(x => !visited(x)).map {
        case n2 if changes.isDefinedAt((n, n2)) => (n2, group.op(index, changes((n, n2))))
        case n2 if changes.isDefinedAt((n2, n)) => (n2, group.op(index, group.inv(changes((n2, n)))))
        case n2 => (n2, index)
      }

      for ((n, ix) <- updates) {
        visited.add(n)
        border.enqueue((n, ix))
      }

      (n, index)
    }
  }


  /**
   * TODO: Replace by the more general index2.
   */
  def index[N, K, V](ns: Set[N], es: Set[(N, N)], cp: Set[N],
                     checkps: Map[N, Map[K, V]], changes: Map[(N, N), Map[K, V]],
                     group: Group[Map[K, V]]): Iterator[(N, Map[K, V])] = new Iterator[(N, Map[K, V])] {

    val visited: mutable.Set[N] = mutable.Set[N]()
    val border: mutable.Queue[(N, Map[K, V])] = mutable.Queue[(N, Map[K, V])]()

    val edges: Map[N, Set[N]] = (es ++ es.map(_.swap)).groupBy(_._1).mapValues(_.map(_._2)).withDefaultValue(Set())

    // Add checkpoints
    for (n <- cp) {
      visited.add(n)
      border.enqueue((n, checkps.getOrElse(n, group.zero())))
    }

    override def hasNext: Boolean = border.nonEmpty

    override def next(): (N, Map[K, V]) = {
      val (n, index) = border.dequeue()
      val updates = edges(n).filter(x => !visited(x)).map {
        case n2 if changes.isDefinedAt((n, n2)) => (n2, group.op(index, changes((n, n2))))
        case n2 if changes.isDefinedAt((n2, n)) => (n2, group.op(index, group.inv(changes((n2, n)))))
        case n2 => (n2, index)
      }

      for ((n, ix) <- updates) {
        visited.add(n)
        border.enqueue((n, ix))
      }

      (n, index)
    }
  }

  def indegree[N](es: Set[(N, N)]): Map[N, Int] = es.groupBy(_._2).map { case (n, ins) => (n, ins.size) }

  def outdegree[N](es: Set[(N, N)]): Map[N, Int] = es.groupBy(_._1).map { case (n, ins) => (n, ins.size) }

  /**
   * A contraction of edges without violating the acyclic graph property
   */
  def accontract[N](ns: Set[N], ess: Set[(N, N)], contraction: Set[(N, N)], cbypass: Boolean = true, phase: Boolean = true): (Set[N], Set[(N, N)], Map[N, N]) = {
    val bpfilter = if (cbypass) bypass(ns, ess).toSet else Set[(N, N)]()
    val es = ess.filter(x => !bpfilter(x))

    val rank = ts(ns, es).zipWithIndex.toMap
    val indegree1 = indegree(es).filter { case (_, degree) => degree == 1 }.keySet
    val outdegree1 = outdegree(es).filter { case (_, degree) => degree == 1 }.keySet

    val components = es.intersect(contraction).filter { case (n1, n2) => (indegree1(n2) && phase) || (outdegree1(n1) && !phase) }
      .map { case (n1, n2) => Set(n1, n2) }

    if (components.isEmpty && bpfilter.isEmpty && !phase)
      return (ns, es, ns.map(n => n -> n).toMap)

    val connectedComponents = ccs(components)

    val leet = groupByValue(connectedComponents.toSeq).map { case (cc, vs) => cc -> vs.minBy(rank) }

    val mapping = ns.map {
      case n if connectedComponents.isDefinedAt(n) => n -> leet(connectedComponents(n))
      case n => n -> n
    }.toMap

    val (es2, ns2, m2) = accontract(
      ns.map(mapping),
      es.map { case (n1, n2) => (mapping(n1), mapping(n2)) }.filter { case (n1, n2) => n1 != n2 },
      contraction.map { case (n1, n2) => (mapping(n1), mapping(n2)) }.filter { case (n1, n2) => n1 != n2 }, cbypass, !phase)


    (es2, ns2, mapping.map { case (n1, n2) => (n1, m2(n2)) })
  }

  def bypass[N](ns: Set[N], es: Set[(N, N)]): Iterator[(N, N)] = {
    val ins = incoming(ns, es)
    val outs = outgoing(ns, es)
    val active = mutable.Map[N, Int]()
    val tcs = mutable.Map[N, Set[N]]()

    (for (n <- ts(ns, es)) yield {
      // Check on bypasses.
      val bypasses = ins(n).flatMap(x => tcs(x).diff(Set(x))).intersect(ins(n))

      // The tc of this node is needed outs(n) times.
      active.put(n, outs(n).size)

      // Construct this tc (only containing nodes with more that one output).
      val tc = ins(n).flatMap(x => tcs.getOrElse(x, Set())) ++ (if (outs(n).size > 1) Set(n) else Set())
      tcs.put(n, tc)

      // Decrease activeness.
      for (in <- ins(n)) active.put(in, active(in) - 1)

      // Clean up previous nodes in necessary.
      val clean = ins(n).filter(active(_) == 0)
      clean.foreach(tcs.remove)
      for (x <- tcs.keySet) tcs.put(x, tcs(x).diff(clean))

      // return bypasses.
      bypasses.map(x => (x, n))
    }).flatten
  }

  def ccs2[N](edges: Set[(N, N)], nodes: Set[N]): Map[N, N] =
    ccs(edges.map { case (l, r) => Set(l, r) } ++ nodes.map(x => Set(x)))


  def ccs[N](es: Set[Set[N]]): Map[N, N] = {
    val cc = mutable.Map[N, N]()
    val icc = mutable.Map[N, Set[N]]()

    for (n <- es.flatten) {
      cc.put(n, n)
      icc.put(n, Set(n))
    }

    def merge(mn: Set[N]): Unit = {
      val m = mn.map(cc)
      if (m.size > 1) {
        val head = m.maxBy(icc(_).size)
        icc.put(head, m.flatMap(n => icc(n)))
        for (n2 <- m.filter(_ != head); n <- icc.remove(n2).get) cc.put(n, head)
      }
    }

    for (m <- es)
      merge(m)

    cc.toMap
  }

  def imap[X, K1, V1, K2, V2](content: Map[X, Map[K1, V1]])(f: (K1, V1) => Map[K2, V2])(implicit ring: Group[V2]): Map[X, Map[K2, V2]] = {
    val abl = GMap[X, Map[K2, V2]](GMap(ring))
    content.toSeq.flatMap { case (n, kvs) => kvs.toSeq.map { case (k, v) => ((k, v), n) } }
      .groupBy(_._1).flatMap { case ((k1, v1), kvns) =>
      val kv2 = f(k1, v1)
      kvns.map { case (_, n) => Map(n -> kv2) }
    }.fold(abl.zero())(abl.op)
  }

  def imap2[X, K1, V1, K2](content: Map[X, Map[K1, V1]])(f: K1 => K2)(implicit ring: Group[V1]): Map[X, Map[K2, V1]] = {
    val abl = GMap[X, Map[K2, V1]](GMap(ring))
    content.toSeq.flatMap { case (n, kvs) => kvs.toSeq.map { case (k, v) => (k, (v, n)) } }
      .groupBy(_._1).flatMap { case (k1, kvns) =>
      val k2 = f(k1)
      kvns.map { case (_, (v2, n)) => Map(n -> Map(k2 -> v2)) }
    }.fold(abl.zero())(abl.op)
  }

  def cmap[X, K1, V1, K2, V2](content: Map[X, Map[K1, V1]])(f: (K1, V1) => Map[K2, V2])(implicit ring: Group[V2]): Map[X, Map[K2, V2]] = {
    val abl = GMap[X, Map[K2, V2]](GMap(ring))
    (for ((n, kvs) <- content; (k, v) <- kvs; (k2, v2) <- f(k, v) if v2 != ring.zero()) yield Map(n -> Map(k2 -> v2))).fold(abl.zero())(abl.op)
  }

  def cmap2[X, K1, V1, K2](content: Map[X, Map[K1, V1]])(f: K1 => K2)(implicit ring: Group[V1]): Map[X, Map[K2, V1]] = {
    val abl = GMap[X, Map[K2, V1]](GMap(ring))
    (for ((n, kvs) <- content; (k, v) <- kvs) yield Map(n -> Map(f(k) -> v))).fold(abl.zero())(abl.op)
  }

}
