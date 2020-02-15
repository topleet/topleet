package org.topleet.engines

import org.topleet.{Dynamics, Engine, Leet, Pairleet, Topleets}
import org.topleet.groups.{GTuple, Group, GMap}

import org.topleet.Dynamics.Dynamics
import org.topleet.groups.{GTuple, Group, GMap}


import scala.collection.{GenIterable, mutable}
import scala.collection.parallel.ParSeq
import scala.collection.parallel.immutable.{ParIterable, ParMap}
import scala.collection.parallel.mutable.ParArray
import scala.reflect.{ClassTag => CT}

/**
 * An incremental and parallel engine still containing some experiments on how to efficiently merge in local parallelization.
 */
object IncrementalParallelEngine {
  def create(): Engine = new IncrementalParallelEngine()
}

case class IncrementalParallelEngine(bulkmerge: Boolean = true, var PAR_LEVEL: Int = 16) extends Engine {

  def SQR_PAR_LEVEL: Int = PAR_LEVEL

  override def create[N: CT, K: CT, V: CT](nodes: () => Set[N], inEdges: N => Set[N], values: N => Map[K, V])(implicit ring: Group[V]): Leet[N, Map[K, V]] = {
    val ns = nodes()
    val es = ns.flatMap(n => inEdges(n).map(in => (in, n)))

    val gmkv = GMap[K, V](ring)

    val newCp = Topleets.ccs2(es, ns).values.toSet
    val newCheckpoints = newCp.par.toSeq.flatMap(n => values(n).map { case (k, v) => (n, k, v) })
    val newChanges = es.par.toSeq.flatMap { case (n1, n2) => gmkv.diff(values(n1), values(n2)).map { case (k, v) => ((n1, n2), k, v) } }

    new IncrementalParallelLeet(ns, es, newCp, newCheckpoints, newChanges, ring)
  }

  class IncrementalParallelLeet[N1: CT, K1: CT, V1: CT](val ns: Set[N1], val es: Set[(N1, N1)], val cp: Set[N1],
                                                        val checkps: ParIterable[(N1, K1, V1)],
                                                        val changes: ParIterable[((N1, N1), K1, V1)],
                                                        val ring: Group[V1]) extends Pairleet[N1, K1, V1] {


    def keyhash(k: Any): Int =
      if (k == null) 0
      else Math.abs(k.hashCode())

    import org.topleet.libs.Natives._

    override def g(): Group[V1] = ring

    override def edges(): Iterator[(N1, N1)] = es.toIterator

    override def nodes(): Iterator[N1] = ns.toIterator

    override def size(): Long = ns.size

    override def topology(): Leet[N1, Map[N1, Int]] = {
      val newCheckps = cp.par.toSeq.map { n => (n, n, 1) }
      val newChanges = es.par.toSeq.flatMap { case (n1, n2) => Seq(((n1, n2), n1, -1), ((n1, n2), n2, 1)) }
      new IncrementalParallelLeet(ns, es, cp, newCheckps, newChanges, aInteger)
    }

    override def index(): Iterator[(N1, Map[K1, V1])] = {
      val ablCheckps1: GMap[N1, Map[K1, V1]] = GMap(GMap(ring))
      val ablChanges1: GMap[(N1, N1), Map[K1, V1]] = GMap(GMap(ring))

      val ichanges = changes.map { case (n, k, v) => Map(n -> Map(k -> v)) }.fold(ablChanges1.zero())(ablChanges1.op)
      val icheckps = checkps.map { case (n, k, v) => Map(n -> Map(k -> v)) }.fold(ablCheckps1.zero())(ablCheckps1.op)

      Topleets.index2(ns, es, cp, icheckps, ichanges, aMap[K1, V1](ring))
    }

    override def diff(merges: Boolean): Leet[N1, Map[K1, V1]] = {
      val ins = Topleets.incoming(ns, es)
      val out = Topleets.outgoing(ns, es)

      def filter(n: N1): Boolean = merges || (ins(n).size == 1)

      val i = for (((n1, n2), k1, v1) <- changes if filter(n2); in <- ins(n2)) yield ((in, n2), k1, v1)
      val o = for (((n1, n2), k1, v1) <- changes if filter(n2); ou <- out(n2)) yield ((n2, ou), k1, ring.inv(v1))

      val c = for (((n1, n2), k1, v1) <- changes if cp(n2) && filter(n2)) yield (n2, k1, v1)

      new IncrementalParallelLeet(ns, es, cp, c, i ++ o, ring).collapse()
    }

    def collapse(): IncrementalParallelLeet[N1, K1, V1] = {
      val newChanges = parChangesOnly(PAR_LEVEL)
        .flatMap { collapsed => for (((n1, n2), kvs) <- collapsed; (k, v) <- kvs) yield ((n1, n2), k, v) }

      val newCheckps = parCheckpsOnly(PAR_LEVEL)
        .flatMap { collapsed => for ((n, kvs) <- collapsed; (k, v) <- kvs) yield (n, k, v) }
      new IncrementalParallelLeet(ns, es, cp, newCheckps, newChanges, ring)
    }

    override def tmapHom[K2: CT, V2: CT](f: (K1, V1) => Map[K2, V2], dynamics: Dynamics.Dynamics)(implicit gv2: Group[V2]): Leet[N1, Map[K2, V2]] = {
      dynamics match {
        case Dynamics.Linear =>
          val newChanges = for ((e, k, v) <- changes; (k2, v2) <- f(k, v) if v2 != gv2.zero()) yield (e, k2, v2)
          val newCheckps = for ((n, k, v) <- checkps; (k2, v2) <- f(k, v) if v2 != gv2.zero()) yield (n, k2, v2)
          new IncrementalParallelLeet(ns, es, cp, newCheckps, newChanges, gv2)

        case Dynamics.Collapse =>
          val collapsed = collapse()
          val newChanges = for ((e, k, v) <- collapsed.changes; (k2, v2) <- f(k, v) if v2 != gv2.zero()) yield (e, k2, v2)
          val newCheckps = for ((n, k, v) <- collapsed.checkps; (k2, v2) <- f(k, v) if v2 != gv2.zero()) yield (n, k2, v2)
          new IncrementalParallelLeet(ns, es, cp, newCheckps, newChanges, gv2)

        case Dynamics.Memoization =>
          val newChanges = parChangesOnly(PAR_LEVEL).flatMap(x => Topleets.imap(x)(f)).flatMap { case (e, kvs) => kvs.map { case (k, v) => (e, k, v) } }
          val newCheckps = parCheckpsOnly(PAR_LEVEL).flatMap(x => Topleets.imap(x)(f)).flatMap { case (n, kvs) => kvs.map { case (k, v) => (n, k, v) } }
          new IncrementalParallelLeet(ns, es, cp, newCheckps, newChanges, gv2)
      }
    }

    override def tmap[K2: CT](f: K1 => K2, dynamics: Dynamics): Leet[N1, Map[K2, V1]] = {
      dynamics match {
        case Dynamics.Linear =>
          val newChanges = changes.map { case (e, k, v) => (e, f(k), v) }
          val newCheckps = checkps.map { case (e, k, v) => (e, f(k), v) }
          new IncrementalParallelLeet(ns, es, cp, newCheckps, newChanges, ring)

        case Dynamics.Collapse =>
          val collapsed = collapse()
          val newChanges = collapsed.changes.map { case (e, k, v) => (e, f(k), v) }
          val newCheckps = collapsed.checkps.map { case (e, k, v) => (e, f(k), v) }
          new IncrementalParallelLeet(ns, es, cp, newCheckps, newChanges, ring)

        case Dynamics.Memoization =>
          implicit val r: Group[V1] = ring
          val newChanges = parChangesOnly(PAR_LEVEL).flatMap(x => Topleets.imap2(x)(f)).flatMap { case (e, kvs) => kvs.map { case (k, v) => (e, k, v) } }
          val newCheckps = parCheckpsOnly(PAR_LEVEL).flatMap(x => Topleets.imap2(x)(f)).flatMap { case (n, kvs) => kvs.map { case (k, v) => (n, k, v) } }
          new IncrementalParallelLeet(ns, es, cp, newCheckps, newChanges, ring)
      }
    }

    def merge2[KK, VV](i: Iterable[(KK, VV)], ring: Group[VV]): Map[KK, VV] = {
      val ii = i.filter { case (k, v) => v != ring.zero() }
      if (bulkmerge) {
        val m = mutable.Map[KK, VV]()
        for ((k, v) <- ii) {
          val next = ring.op(m.getOrElse(k, ring.zero()), v)
          if (next == ring.zero()) m.remove(k)
          else m.put(k, next)
        }
        m.toMap
      }
      else {
        val abl: GMap[KK, VV] = GMap(ring)

        ii.map { case (k, v) => Map(k -> v) }.fold(abl.zero())(abl.op)
      }
    }

    def merge3[NN, KK, VV](i: Iterable[(NN, KK, VV)], ring: Group[VV]): Map[NN, Map[KK, VV]] = {
      val ii = i.filter { case (n, k, v) => v != ring.zero() }
      if (bulkmerge) {
        val m = mutable.Map[NN, mutable.Map[KK, VV]]()
        for ((n, k, v) <- ii) {
          val mm = m.getOrElseUpdate(n, mutable.Map())
          val next = ring.op(mm.getOrElse(k, ring.zero()), v)
          if (next == ring.zero()) {
            mm.remove(k)
            if (mm.isEmpty)
              m.remove(n)
          }
          else mm.put(k, next)
        }
        m.map { case (k, v) => (k, v.toMap) }.toMap
      }
      else {
        val abl: GMap[NN, Map[KK, VV]] = GMap(GMap(ring))

        ii.map { case (n, k, v) => Map(n -> Map(k -> v)) }.fold(abl.zero())(abl.op)
      }
    }

    override def tfilter(f: K1 => Boolean): Leet[N1, Map[K1, V1]] = {
      val newChanges = changes.filter { case (e, k, v) => f(k) }
      val newCheckps = checkps.filter { case (e, k, v) => f(k) }
      new IncrementalParallelLeet(ns, es, cp, newCheckps, newChanges, ring)
    }

    def parCheckpsOnly(pl: Int): ParIterable[Map[N1, Map[K1, V1]]] =
      checkps.groupBy { case (_, k, _) => keyhash(k) % pl }.map { case (_, x) =>
        merge3(x.seq, ring)
      }


    def parChangesOnly(pl: Int): ParIterable[Map[(N1, N1), Map[K1, V1]]] =
      changes.groupBy { case (_, k, _) => keyhash(k) % pl }.map { case (_, x) =>
        merge3(x.seq, ring)
      }


    def parChangesAndCheckps(pl: Int): ParMap[Map[(N1, N1), Map[K1, V1]], Map[N1, Map[K1, V1]]] = {

      val paired = checkps.map(x => (Option(x), None)) ++ changes.map(x => (None, Option(x)))

      val grouped = paired.groupBy {
        case (Some((n, k, v)), None) => keyhash(k) % pl
        case (None, Some((n, k, v))) => keyhash(k) % pl
      }

      grouped.map { case (_, x) =>
        val changes = merge3(x.flatMap(_._2).seq, ring)
        val checkps = merge3(x.flatMap(_._1).seq, ring)
        (changes, checkps)
      }
    }

    override def tmapIx[K2: CT, V2: CT](f: (K1, V1) => Map[K2, V2], dynamics: Dynamics.Dynamics)(implicit gv2: Group[V2]): Leet[N1, Map[K2, V2]] = {
      val ins = Topleets.incoming(ns, es)
      val out = Topleets.outgoing(ns, es)
      // TODO: Dynamics missing.
      val newChanges = parChangesAndCheckps(PAR_LEVEL).flatMap { case (ichanges, icheckps) =>
        (for ((n, ix) <- Topleets.index(ns, es, cp, icheckps, ichanges, aMap[K1, V1](ring));
              in <- ins(n).toSeq; (k, _) <- ichanges.getOrElse((in, n), Map()).toSeq if ix.isDefinedAt(k); (k2, v2) <- f(k, ix(k)).toSeq if v2 != gv2.zero())
          yield ((in, n), k2, v2)) ++
          (for ((n, ix) <- Topleets.index(ns, es, cp, icheckps, ichanges, aMap[K1, V1](ring));
                ou <- out(n).toSeq; (k, _) <- ichanges.getOrElse((n, ou), Map()).toSeq if ix.isDefinedAt(k); (k2, v2) <- f(k, ix(k)).toSeq if v2 != gv2.zero())
            yield ((n, ou), k2, gv2.inv(v2)))
      }

      val newCheckps = parCheckpsOnly(PAR_LEVEL).flatMap { icheckps =>
        for ((n, kvs) <- icheckps; (k, v) <- kvs; (k2, v2) <- f(k, v).toSeq if v2 != gv2.zero())
          yield (n, k2, v2)
      }

      new IncrementalParallelLeet(ns, es, cp, newCheckps, newChanges, gv2)
    }

    override def tmapIx2[K2: CT, V2: CT](f: (K1, V1, V1) => Map[K2, V2], dynamics: Dynamics.Dynamics)(implicit gv2: Group[V2]): Leet[N1, Map[K2, V2]] = {
      val ins = Topleets.incoming(ns, es)
      // TODO: Dynamics missing.
      val newChanges = parChangesAndCheckps(PAR_LEVEL).flatMap { case (ichanges, icheckps) =>
        for ((n, ix) <- Topleets.index(ns, es, cp, icheckps, ichanges, aMap[K1, V1](ring));
             in <- ins(n).toSeq; (k, cv) <- ichanges.getOrElse((in, n), Map()).toSeq; (k2, v2) <- f(k, ix.getOrElse(k, ring.zero()), cv).toSeq if v2 != gv2.zero())
          yield ((in, n), k2, v2)
      }

      val newCheckps = parCheckpsOnly(PAR_LEVEL).flatMap { icheckps =>
        for ((n, kvs) <- icheckps; (k, v) <- kvs; (k2, v2) <- f(k, v, v).toSeq if v2 != gv2.zero())
          yield (n, k2, v2)
      }

      new IncrementalParallelLeet(ns, es, cp, newCheckps, newChanges, gv2)
    }

    override def tadd(l: Leet[N1, Map[K1, V1]]): Leet[N1, Map[K1, V1]] = {
      val l1 = this
      val l2 = l.asInstanceOf[IncrementalParallelLeet[N1, K1, V1]]

      // Currently just handling full overlap and no overlap.
      if (l1.ns == l2.ns && l1.es == l2.es && l1.cp == l2.cp) {
        new IncrementalParallelLeet(l1.ns, l1.es, l1.cp, l1.checkps ++ l2.checkps, l1.changes ++ l2.changes, ring)
      } else if (l1.ns.intersect(l2.ns).isEmpty) {
        new IncrementalParallelLeet(l1.ns.union(l2.ns), l1.es.union(l2.es), l1.cp.union(l2.cp), l1.checkps ++ l2.checkps, l1.changes ++ l2.changes, ring)
      } else ???

    }

    override def tcartesian[K2: CT](l: Leet[N1, Map[K2, V1]]): Leet[N1, Map[(K1, K2), V1]] = {
      // TODO: Change something for decreasing the high construction of maps here. Increase parallelism?
      val ins = Topleets.incoming(ns, es)
      val abl = l.g()

      // The same as for the RDD Engine collapse?
      val l1 = this //.collapse()
      val l2 = l.asInstanceOf[IncrementalParallelLeet[N1, K2, V1]] //.collapse()

      // TODO: Might be possible to implement using parchange and parcheckpoints.
      // Currently just handling full overlap of the topology.
      if (l1.ns == l2.ns && l1.es == l2.es && l1.cp == l2.cp) {
        val ablCheckps: GMap[N1, (Map[K1, V1], Map[K2, V1])] = GMap(GTuple(aMap[K1, V1](ring), aMap[K2, V1](ring)))
        val ablChanges: GMap[(N1, N1), (Map[K1, V1], Map[K2, V1])] = GMap(GTuple(aMap[K1, V1](ring), aMap[K2, V1](ring)))

        val zipped1 = l1.parChangesAndCheckps(SQR_PAR_LEVEL)

        val zipped2 = l2.parChangesAndCheckps(SQR_PAR_LEVEL)
        var i = 0

        val newChanges = (for ((changes1, checkps1) <- zipped1; (changes2, checkps2) <- zipped2) yield {
          def xindex() = Topleets.index(ns, es, cp, checkps1, changes1, aMap[K1, V1](ring))

          def yindex() = Topleets.index(ns, es, cp, checkps2, changes2, aMap[K2, V1](ring))

          def left = (for ((n, ix) <- xindex(); in <- ins(n).toSeq; (k2, v2) <- changes2.getOrElse((in, n), Map())) yield
            ix.map { case (k1, v1) => ((in, n), (k1, k2), abl.mop(v1, v2)) }).flatten

          def right = (for ((n, ix) <- yindex(); in <- ins(n).toSeq; (k1, v1) <- changes1.getOrElse((in, n), Map())) yield
            ix.map { case (k2, v2) => ((in, n), (k1, k2), abl.mop(v1, v2)) }).flatten

          def inner = for (n <- ns.toIterator; in <- ins(n).toSeq;
                           (k1, v1) <- changes1.getOrElse((in, n), Map());
                           (k2, v2) <- changes2.getOrElse((in, n), Map())) yield
            ((in, n), (k1, k2), abl.minv(abl.mop(v1, v2)))

          left ++ right ++ inner

        }).flatten

        val newCheckps = (for (checkps1 <- l1.parCheckpsOnly(SQR_PAR_LEVEL); checkps2 <- l2.parCheckpsOnly(SQR_PAR_LEVEL)) yield {
          for (n <- cp.toSeq if checkps1.isDefinedAt(n) && checkps2.isDefinedAt(n); (k1, v1) <- checkps1(n).toSeq; (k2, v2) <- checkps2(n).toSeq)
            yield (n, (k1, k2), abl.mop(v1, v2))
        }).flatten

        new IncrementalParallelLeet(ns, es, cp, newCheckps, newChanges, ring)
      } else
        ???
    }
  }

}
