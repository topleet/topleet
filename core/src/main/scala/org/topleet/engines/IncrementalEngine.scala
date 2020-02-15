package org.topleet.engines

import org.topleet.Dynamics.Dynamics
import org.topleet.groups.{Group, GMap}
import org.topleet.{Dynamics, Engine, Leet, Pairleet, Topleets}

import scala.reflect.{ClassTag => CT}

/**
 * An very basic incremental engine.
 */
object IncrementalEngine extends Engine {

  override def create[N: CT, K: CT, V: CT](nodes: () => Set[N], inEdges: N => Set[N], values: N => Map[K, V])(implicit ring: Group[V]): Leet[N, Map[K, V]] = {
    val ns = nodes()
    val es = ns.flatMap(n => inEdges(n).map(in => (in, n)))

    val gmkv = GMap[K, V](ring)

    val newCp = Topleets.ccs2(es, ns).values.toSet
    val newCheckpoints = newCp.map(x => x -> values(x)).toMap
    val newChanges = es.map { case (n1, n2) => (n1, n2) -> gmkv.diff(values(n1), values(n2)) }.toMap

    new IncrementalLeet(ns, es, newCp, newCheckpoints, newChanges, ring)
  }
}

class IncrementalLeet[N1: CT, K1: CT, V1: CT](val ns: Set[N1], val es: Set[(N1, N1)], val cp: Set[N1],
                                              val checkps: Map[N1, Map[K1, V1]], val changes: Map[(N1, N1), Map[K1, V1]],
                                              val gv1: Group[V1]) extends Pairleet[N1, K1, V1] {

  import org.topleet.libs.Natives._

  override def g(): Group[V1] = gv1

  override def edges(): Iterator[(N1, N1)] = es.toIterator

  override def nodes(): Iterator[N1] = ns.toIterator

  override def size(): Long = ns.size

  override def topology(): Leet[N1, Map[N1, Int]] = {
    val newCheckps = cp.map { n => n -> Map(n -> 1) }.toMap
    val newChanges = es.map { case (n1, n2) => (n1, n2) -> Map(n1 -> -1, n2 -> 1) }.toMap
    new IncrementalLeet(ns, es, cp, newCheckps, newChanges, aInteger)
  }

  override def index(): Iterator[(N1, Map[K1, V1])] =
    Topleets.index(ns, es, cp, checkps, changes, aMap(gv1))

  override def diff(merges: Boolean): Leet[N1, Map[K1, V1]] = {
    val ablCheckps: GMap[N1, Map[K1, V1]] = GMap(GMap(gv1))
    val ablChanges: GMap[(N1, N1), Map[K1, V1]] = GMap(GMap(gv1))

    val ins = Topleets.incoming(ns, es)
    val out = Topleets.outgoing(ns, es)

    def filter(n: N1): Boolean = merges || (ins(n).size == 1)

    val i = for (((n1, n2), kvs) <- changes.toIterator if filter(n2); (k1, v1) <- kvs.toIterator; in <- ins(n2).toIterator) yield Map((in, n2) -> Map(k1 -> v1))
    val o = for (((n1, n2), kvs) <- changes.toIterator if filter(n2); (k1, v1) <- kvs.toIterator; ou <- out(n2).toIterator) yield Map((n2, ou) -> Map(k1 -> gv1.inv(v1)))

    val c = for (c <- cp.toIterator if filter(c); n1 <- ins(c).toIterator; (k1, v1) <- changes.getOrElse((n1, c), Map()).toIterator) yield Map(c -> Map(k1 -> v1))

    val newChanges = (i ++ o).fold(ablChanges.zero())(ablChanges.op)
    val newCheckps = c.fold(ablCheckps.zero())(ablCheckps.op)

    new IncrementalLeet(ns, es, cp, newCheckps, newChanges, gv1)
  }

  override def tmapHom[K2: CT, V2: CT](f: (K1, V1) => Map[K2, V2], dynamics: Dynamics = Dynamics.Collapse)(implicit gv2: Group[V2]): Leet[N1, Map[K2, V2]] = {
    val abl: GMap[K2, V2] = GMap(gv2)

    val newChanges = (for ((n, kvs) <- changes) yield {
      n -> (for ((k, v) <- kvs.toSeq; (k2, v2) <- f(k, v) if v2 != gv2.zero()) yield Map(k2 -> v2)).fold(abl.zero())(abl.op)
    }).filter { case (_, kvs) => kvs != abl.zero() }

    val newCheckps = (for ((n, kvs) <- checkps) yield {
      n -> (for ((k, v) <- kvs.toSeq; (k2, v2) <- f(k, v) if v2 != gv2.zero()) yield Map(k2 -> v2)).fold(abl.zero())(abl.op)
    }).filter { case (_, kvs) => kvs != abl.zero() }

    new IncrementalLeet(ns, es, cp, newCheckps, newChanges, gv2)
  }

  override def tmapIx[K2: CT, V2: CT](f: (K1, V1) => Map[K2, V2], dynamics: Dynamics = Dynamics.Collapse)(implicit gv2: Group[V2]): Leet[N1, Map[K2, V2]] = {
    val ins = Topleets.incoming(ns, es)
    val out = Topleets.outgoing(ns, es)

    val ablCheckps: GMap[N1, Map[K2, V2]] = GMap(GMap(gv2))
    val ablChanges: GMap[(N1, N1), Map[K2, V2]] = GMap(GMap(gv2))

    // TODO: Use one run on index.
    // TODO: No dynamics here.

    val i = for ((n, ix) <- index(); in <- ins(n).toSeq; (k, _) <- changes.getOrElse((in, n), Map()).toSeq if ix.isDefinedAt(k); (k2, v2) <- f(k, ix(k)).toSeq if v2 != gv2.zero())
      yield Map((in, n) -> Map(k2 -> v2))

    val o = for ((n, ix) <- index(); ou <- out(n).toSeq; (k, _) <- changes.getOrElse((n, ou), Map()).toSeq if ix.isDefinedAt(k); (k2, v2) <- f(k, ix(k)).toSeq if v2 != gv2.zero())
      yield Map((n, ou) -> Map(k2 -> gv2.inv(v2)))

    val newChanges = (i ++ o).fold(ablChanges.zero())(ablChanges.op)

    val newCheckps = (for ((n, kvs) <- checkps.toSeq; (k, v) <- kvs; (k2, v2) <- f(k, v).toSeq if v2 != gv2.zero()) yield Map(n -> Map(k2 -> v2))).fold(ablCheckps.zero())(ablCheckps.op)

    new IncrementalLeet(ns, es, cp, newCheckps, newChanges, gv2)
  }

  /**
   * Toplological map with available index and change but producing changes
   */
  override def tmapIx2[K2: CT, V2: CT](f: (K1, V1, V1) => Map[K2, V2], dynamics: Dynamics = Dynamics.Collapse)(implicit gv2: Group[V2]): Leet[N1, Map[K2, V2]] = {
    val ins = Topleets.incoming(ns, es)

    val ablCheckps: GMap[N1, Map[K2, V2]] = GMap(GMap(gv2))
    val ablChanges: GMap[(N1, N1), Map[K2, V2]] = GMap(GMap(gv2))

    // TODO: No dynamics here.
    val newChanges = (for ((n, ix) <- index(); in <- ins(n).toSeq; (k, cv) <- changes.getOrElse((in, n), Map()).toSeq; (k2, v2) <- f(k, ix.getOrElse(k, gv1.zero()), cv).toSeq if v2 != gv2.zero())
      yield Map((in, n) -> Map(k2 -> v2)))
      .fold(ablChanges.zero())(ablChanges.op)

    val newCheckps = (for ((n, kvs) <- checkps.toSeq; (k, v) <- kvs; (k2, v2) <- f(k, v, v).toSeq if v2 != gv2.zero()) yield Map(n -> Map(k2 -> v2))).fold(ablCheckps.zero())(ablCheckps.op)

    new IncrementalLeet(ns, es, cp, newCheckps, newChanges, gv2)
  }

  override def tadd(l: Leet[N1, Map[K1, V1]]): Leet[N1, Map[K1, V1]] = {
    val ablCheckps: GMap[N1, Map[K1, V1]] = GMap(GMap(gv1))
    val ablChanges: GMap[(N1, N1), Map[K1, V1]] = GMap(GMap(gv1))
    val l1 = this
    val l2 = l.asInstanceOf[IncrementalLeet[N1, K1, V1]]

    // Currently just handling full overlap and no overlap.
    if (l1.ns == l2.ns && l1.es == l2.es && l1.cp == l2.cp) {
      new IncrementalLeet(l1.ns, l1.es, l1.cp, ablCheckps.op(l1.checkps, l2.checkps), ablChanges.op(l1.changes, l2.changes), gv1)
    } else if (l1.ns.intersect(l2.ns).isEmpty) {
      new IncrementalLeet(l1.ns.union(l2.ns), l1.es.union(l2.es), l1.cp.union(l2.cp), ablCheckps.op(l1.checkps, l2.checkps), ablChanges.op(l1.changes, l2.changes), gv1)
    } else ???

  }

  def asLOCLeet[NN, KK, VV](l: Leet[NN, Map[KK, VV]]): IncrementalLeet[NN, KK, VV] =
    l.asInstanceOf[IncrementalLeet[NN, KK, VV]]


  override def tcartesian[K2: CT](l: Leet[N1, Map[K2, V1]]): Leet[N1, Map[(K1, K2), V1]] = {

    val abl = l.g()
    val ins = Topleets.incoming(ns, es)

    val l1 = this
    val l2 = l.asInstanceOf[IncrementalLeet[N1, K2, V1]]

    // Currently just handling full overlap.
    if (l1.ns == l2.ns && l1.es == l2.es && l1.cp == l2.cp) {

      val left = for ((n, ix) <- l1.index(); in <- ins(n).toSeq; (k2, v2) <- l2.changes.getOrElse((in, n), Map())) yield
        ix.map { case (k1, v1) => Map((in, n) -> Map((k1, k2) -> abl.mop(v1, v2))) }

      val right = for ((n, ix) <- l2.index(); in <- ins(n).toSeq; (k1, v1) <- l1.changes.getOrElse((in, n), Map())) yield
        ix.map { case (k2, v2) => Map((in, n) -> Map((k1, k2) -> abl.mop(v1, v2))) }

      val inner = for (n <- ns; in <- ins(n).toSeq;
                       (k1, v1) <- l1.changes.getOrElse((in, n), Map());
                       (k2, v2) <- l2.changes.getOrElse((in, n), Map())) yield
        Map((in, n) -> Map((k1, k2) -> abl.minv(abl.mop(v1, v2))))

      val c = for (n <- cp.toSeq if l1.checkps.isDefinedAt(n) && l2.checkps.isDefinedAt(n); (k1, v1) <- l1.checkps(n).toSeq; (k2, v2) <- l2.checkps(n).toSeq)
        yield Map(n -> Map((k1, k2) -> abl.mop(v1, v2)))

      val ablCheckps: GMap[N1, Map[(K1, K2), V1]] = GMap(GMap(gv1))
      val ablChanges: GMap[(N1, N1), Map[(K1, K2), V1]] = GMap(GMap(gv1))

      val newChanges = (left.flatten ++ right.flatten ++ inner).fold(ablChanges.zero())(ablChanges.op)
      val newCheckps = c.fold(ablCheckps.zero())(ablCheckps.op)

      new IncrementalLeet(ns, es, cp, newCheckps, newChanges, gv1)

    } else
      ???
  }

  override def tmap[K2: CT](f: K1 => K2, dynamics: Dynamics): Leet[N1, Map[K2, V1]] = {
    implicit val av1: Group[V1] = g()
    dynamics match {
      case Dynamics.Memoization =>
        val newChanges = Topleets.imap2(changes)(f)
        val newCheckpoints = Topleets.imap2(checkps)(f)
        new IncrementalLeet(ns, es, cp, newCheckpoints, newChanges, gv1)
      case _ => tmapHom({ case (k, v) => Map(f(k) -> v) }, dynamics)
    }
  }

  override def tfilter(f: K1 => Boolean): Leet[N1, Map[K1, V1]] = {
    implicit val av1: Group[V1] = g()

    val newChanges = changes.map { case (n, kvs) => (n, kvs.filter { case (k, _) => f(k) }) }
    val newCheckps = checkps.map { case (n, kvs) => (n, kvs.filter { case (k, _) => f(k) }) }

    new IncrementalLeet(ns, es, cp, newCheckps, newChanges, gv1)
  }

}
