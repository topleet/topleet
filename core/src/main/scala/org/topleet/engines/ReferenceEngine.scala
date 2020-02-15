package org.topleet.engines

import org.topleet.Dynamics.Dynamics
import org.topleet.groups.{Group, GMap}
import org.topleet._

import scala.reflect.{ClassTag => CT}

/**
  * Reference engine working with no optimizations. This engine should be used for checking other engines.
  */
object ReferenceEngine extends Engine {
  override def create[N: CT, K: CT, V: CT](nodes: () => Set[N], inEdges: N => Set[N], values: N => Map[K, V])(implicit ring: Group[V]): Leet[N, Map[K, V]] = {
    val ns = nodes()
    val es = ns.flatMap(n => inEdges(n).map(in => (in, n)))

    new ReferenceLeet(ns.map(n => (n, values(n))).filter { case (n, kvs) => kvs.nonEmpty }.toMap, ns, es, ring)
  }
}

class ReferenceLeet[N1: CT, K1: CT, V1: CT](val values: Map[N1, Map[K1, V1]], val ns: Set[N1], val es: Set[(N1, N1)], val gv1: Group[V1]) extends Pairleet[N1, K1, V1] {
  override def g(): Group[V1] = gv1

  import org.topleet.libs.Natives._

  override def edges(): Iterator[(N1, N1)] = es.toIterator

  override def nodes(): Iterator[N1] = ns.toIterator

  override def size(): Long = ns.size

  override def topology(): Leet[N1, Map[N1, Int]] =
    new ReferenceLeet(ns.map { n => (n, Map(n -> 1)) }.toMap, ns, es, aInteger)

  override def index(): Iterator[(N1, Map[K1, V1])] = ns.toIterator.map(n => (n, values.getOrElse(n, Map())))

  override def diff(merges: Boolean): Leet[N1, Map[K1, V1]] = {
    val abl = aMap[K1, V1](gv1)
    val aout = aMap[N1, Map[K1, V1]](aMap(gv1))
    val ins = Topleets.incoming(ns, es)

    def filter(n: N1): Boolean = merges || (ins(n).size == 1)

    val newNodes = (for ((n1, n2) <- edges() if filter(n2); (k1, v1) <- abl.diff(values.getOrElse(n1, abl.zero()), values.getOrElse(n2, abl.zero())))
      yield Map(n2 -> Map( k1 -> v1))).fold(aout.zero())(aout.op)

    new ReferenceLeet(newNodes, ns, es, gv1)
  }

  override def tmapIx2[K2: CT, V2: CT](f: (K1, V1, V1) => Map[K2, V2], dynamics: Dynamics.Dynamics = Dynamics.Collapse)(implicit gv2: Group[V2]): Leet[N1, Map[K2, V2]] =
    tmapIx({ case (k, v) => f(k, v, v) }, dynamics)

  override def tmapHom[K2: CT, V2: CT](f: (K1, V1) => Map[K2, V2], dynamics: Dynamics.Dynamics = Dynamics.Collapse)(implicit gv2: Group[V2]): Leet[N1, Map[K2, V2]] =
    tmapIx({ case (k1, v1) => f(k1, v1) }, dynamics)

  override def tadd(l: Leet[N1, Map[K1, V1]]): Leet[N1, Map[K1, V1]] = {
    val ll = l.asInstanceOf[ReferenceLeet[N1, K1, V1]]

    val abl: GMap[N1, Map[K1, V1]] = GMap(GMap[K1, V1](gv1))

    new ReferenceLeet(abl.op(values, ll.values), ns.union(ll.ns), es.union(ll.es), gv1)
  }

  override def tcartesian[K2: CT](l: Leet[N1, Map[K2, V1]]): Leet[N1, Map[(K1, K2), V1]] = {
    val ll: ReferenceLeet[N1, K2, V1] = l.asInstanceOf[ReferenceLeet[N1, K2, V1]]

    val abl: GMap[N1, Map[(K1, K2), V1]] = GMap(GMap(gv1))
    val newLocalNodes = (for (n <- ns.intersect(ll.ns).toSeq;
                              (k1, v1) <- values.getOrElse(n, Map());
                              (k2, v2) <- ll.values.getOrElse(n, Map())) yield Map(n -> Map((k1, k2) -> l.g().mop(v1, v2))))
      .fold(abl.zero())(abl.op)

    new ReferenceLeet(newLocalNodes, ns.intersect(ll.ns), es.intersect(ll.es), gv1)
  }

  override def tmapIx[K2: CT, V2: CT](f: (K1, V1) => Map[K2, V2], dynamics: Dynamics.Dynamics = Dynamics.Collapse)(implicit gv2: Group[V2]): Leet[N1, Map[K2, V2]] = {
    val newNodes = dynamics match {
      case Dynamics.Memoization => Topleets.imap(values)(f)
      case _ => Topleets.cmap(values)(f)
    }
    new ReferenceLeet(newNodes, ns, es, gv2)
  }

  override def tmap[K2: CT](f: K1 => K2, dynamics: Dynamics = Dynamics.Collapse): Leet[N1, Map[K2, V1]] = {
    implicit val abl: Group[V1] = gv1
    val newNodes = dynamics match {
      case Dynamics.Memoization => Topleets.imap2(values)(f)
      case _ => Topleets.cmap2(values)(f)
    }

    new ReferenceLeet(newNodes, ns, es, gv1)
  }

  override def tfilter(f: K1 => Boolean): Leet[N1, Map[K1, V1]] =
    new ReferenceLeet(values.map { case (n, kvs) => (n, kvs.filterKeys(f)) }, ns, es, gv1)
}