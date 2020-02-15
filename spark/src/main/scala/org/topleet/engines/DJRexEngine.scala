package org.topleet.engines

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.topleet.Dynamics.Dynamics
import org.topleet.groups.Group
import org.topleet.{Dynamics, Engine, Leet, Pairleet}

import scala.reflect.{ClassTag => CT}

/**
  * Reference engine working with no optimizations but distribution and memoization. Should be used for checking other engines
 *  and after calling diff (still in progress).
  */
class DJRexEngine(@transient val spark: SparkContext, val partitions: Int) extends Engine {
  override def create[N: CT, K: CT, V: CT](nodes: () => Set[N], inEdges: N => Set[N], values: N => Map[K, V])(implicit ring: Group[V]): Leet[N, Map[K, V]] = {

    val newNs = spark.parallelize(nodes().toSeq, partitions)
    val newEs = newNs.flatMap(n => inEdges(n).map(in => (in, n)))
    val newValues = newNs.flatMap { n => values(n).map { case (k, v) => ((n, k), v) } }

    new DJRexLeet(newValues, newNs, newEs, ring, partitions)
  }
}

class DJRexLeet[N1: CT, K1: CT, V1: CT](val values: RDD[((N1, K1), V1)],
                                        val ns: RDD[N1],
                                        val es: RDD[(N1, N1)],
                                        val ring: Group[V1],
                                        val partitions: Int) extends Pairleet[N1, K1, V1] {

  override def g(): Group[V1] = ring

  import org.topleet.libs.Natives._

  override def edges(): Iterator[(N1, N1)] = es.toLocalIterator

  override def nodes(): Iterator[N1] = ns.toLocalIterator

  override def size(): Long = ns.count()

  override def topology(): Leet[N1, Map[N1, Int]] = {
    val newValues = ns.map(n => ((n, n), 1))
    new DJRexLeet(newValues, ns, es, aInteger, partitions)
  }

  override def index(): Iterator[(N1, Map[K1, V1])] = {
    val abl = aMap[K1, V1](ring)
    (values.map { case ((n, k), v) => (n, Map(k -> v)) } ++ ns.map { n => (n, abl.zero()) })
      .aggregateByKey(abl.zero())((l, r) => abl.op(l, r), (l, r) => abl.op(l, r))
      .toLocalIterator
    //.reduceByKey(abl.add).toLocalIterator
  }

  def collapse(): DJRexLeet[N1, K1, V1] = {
    val newValues = values
      .aggregateByKey(ring.zero())((l, r) => ring.op(l, r), (l, r) => ring.op(l, r))
      //      .reduceByKey(ring.add)
      .filter { case ((_, _), v) => ring.zero() != v }

    new DJRexLeet(newValues, ns, es, ring, partitions)
  }


  override def diff(merges: Boolean): Leet[N1, Map[K1, V1]] = {
    val ins = es.map(_.swap).groupByKey()
    val prepValues = values.map { case ((n, k), v) => (n, (k, v)) }
    val in = es.join(prepValues).map { case (n1, (n2, (k1, v1))) => (n2, (k1, ring.inv(v1))) }
    val ou = es.map(_.swap).join(prepValues).map { case (n2, (n1, (k1, v1))) => (n2, (k1, v1)) }
    val newValues = (in ++ ou)
    val out = newValues
      .join(ins)
      .collect { case (n, ((k, v), ins)) if ins.size == 1 || merges => ((n, k), v) }

    new DJRexLeet(out, ns, es, ring, partitions).collapse()
  }

  override def tmapIx2[K2: CT, V2: CT](f: (K1, V1, V1) => Map[K2, V2], dynamics: Dynamics.Dynamics = Dynamics.Collapse)(implicit gv2: Group[V2]): Leet[N1, Map[K2, V2]] =
    tmapIx({ case (k, v) => f(k, v, v) }, dynamics)

  override def tadd(l: Leet[N1, Map[K1, V1]]): Leet[N1, Map[K1, V1]] = {
    val l1 = this
    val l2 = l.asInstanceOf[DJRexLeet[N1, K1, V1]]

    val newNs = if (l1.ns == l2.ns) l1.ns else l1.ns.union(l2.ns)
    val newEs = if (l1.es == l2.es) l1.es else l1.es.union(l2.es)

    new DJRexLeet(l1.values ++ l2.values, newNs, newEs, ring, math.max(l1.partitions, l2.partitions))
  }

  override def tcartesian[K2: CT](l: Leet[N1, Map[K2, V1]]): Leet[N1, Map[(K1, K2), V1]] = {
    val l1 = this.collapse()
    val l2 = l.asInstanceOf[DJRexLeet[N1, K2, V1]].collapse()

    val prepValues1 = l1.values.map { case ((n, k), v) => (n, (k, v)) }
    val prepValues2 = l2.values.map { case ((n, k), v) => (n, (k, v)) }

    val newValues = prepValues1.cogroup(prepValues2).flatMap { case (n, (ls, rs)) =>
      for ((k1, v1) <- ls; (k2, v2) <- rs) yield ((n, (k1, k2)), ring.mop(v1, v2))
    }

    new DJRexLeet(newValues, ns, es, ring, partitions)
  }

  override def tmapIx[K2: CT, V2: CT](f: (K1, V1) => Map[K2, V2], dynamics: Dynamics.Dynamics = Dynamics.Collapse)(implicit gv2: Group[V2]): Leet[N1, Map[K2, V2]] = {
    dynamics match {
      case Dynamics.Memoization =>
        val newValues = collapse().values.map { case ((n, k), v) => ((k, v), n) }
          .groupByKey()
          .flatMap { case ((k, v), ns) => for ((k2, v2) <- f(k, v) if v2 != gv2.zero(); n <- ns) yield ((n, k2), v2) }
        new DJRexLeet(newValues, ns, es, gv2, partitions)
      case _ =>
        val newValues = this.collapse().values.flatMap { case ((n, k), v) => f(k, v).collect { case (k2, v2) if v2 != gv2.zero() => ((n, k2), v2) } }
        new DJRexLeet(newValues, ns, es, gv2, partitions)
    }
  }

  override def tmapHom[K2: CT, V2: CT](f: (K1, V1) => Map[K2, V2], dynamics: Dynamics.Dynamics = Dynamics.Collapse)(implicit gv2: Group[V2]): Leet[N1, Map[K2, V2]] = {
    dynamics match {
      case Dynamics.Linear =>
        val newValues = values.flatMap { case ((n, k), v) => f(k, v).map { case (k2, v2) => ((n, k2), v2) } }
        new DJRexLeet(newValues, ns, es, gv2, partitions)
      case _ => tmapIx({ case (k1, v1) => f(k1, v1) }, dynamics)
    }
  }

  override def tmap[K2: CT](f: K1 => K2, dynamics: Dynamics = Dynamics.Collapse): Leet[N1, Map[K2, V1]] = {
    dynamics match {
      case Dynamics.Memoization =>
        val abl = aMap[N1, V1](ring)
        val newValues = values.map { case ((n, k), v) => (k, Map(n -> v)) }
          .aggregateByKey(abl.zero())((l, r) => abl.op(l, r), (l, r) => abl.op(l, r))
          //.reduceByKey(aMap[N1, V1](ring).add)
          .flatMap { case (k, nvs) =>
            val fk = f(k)
            for ((n, v) <- nvs) yield ((n, fk), v)
          }
        new DJRexLeet(newValues, ns, es, ring, partitions)
      case _ =>
        implicit val abl: Group[V1] = ring
        tmapHom({ case (k, v) => Map(f(k) -> v) }, dynamics)
    }
  }

  override def tfilter(f: K1 => Boolean): Leet[N1, Map[K1, V1]] = {
    val newValues = values.filter { case ((n, k), v) => f(k) }
    new DJRexLeet(newValues, ns, es, ring, partitions)
  }
}