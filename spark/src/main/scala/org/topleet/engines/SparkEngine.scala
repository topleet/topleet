package org.topleet.engines

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkContext}
import org.topleet.Dynamics.Dynamics
import org.topleet.groups.{Group, GMap}

import org.topleet.{Dynamics, Engine, Leet, Pairleet, Topleets}

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.reflect.{ClassTag => CT}

/**
 * A distributed, incremental and parallel engine.
 */
object SparkEngine {

  /**
   * The create call adds a [[org.topleet.engines.NodeAliasEngine]] and [[org.topleet.engines.PartitionGraphEngine]]
   * above the SparkEngine, for optimized network communication and better distribution of huge topology.
   * @param nodes Assumed number of nodes in the processed topology (still needs to be evaluated).
   * @param spark Spark instance.
   * @return The engine.
   */
  def create(nodes: Int, spark: SparkContext): Engine = {

    val keys = ((1.0 / 7) * math.pow(nodes, 0.7) + 64).toInt
    val components = ((1.0 / 6000) * math.pow(nodes, 1.15) + 1).toInt

    NodeAliasEngine(PartitionGraphEngine(SparkEngine(spark, keys), components, 1))
  }
}


case class SparkEngine(@transient val spark: SparkContext, val partitions: Int) extends Engine {

  import org.topleet.libs.Natives._

  override def create[N: CT, K: CT, V: CT](nodes: () => Set[N], inEdges: N => Set[N], values: N => Map[K, V])(implicit ring: Group[V]): Leet[N, Map[K, V]] = {
    val nod = nodes()
    val ess = nod.flatMap(n => inEdges(n).map(in => (in, n)))

    val gmkv = GMap[K, V](ring)

    val cps = Topleets.ccs2(ess, nod).values.toSet
    val newCheckpoints = spark.parallelize(cps.toSeq, partitions).flatMap(n => values(n).map { case (k, v) => (k, (n, v)) })
    val newChanges = spark.parallelize(ess.toSeq, partitions).flatMap { case (n1, n2) => gmkv.diff(values(n1), values(n2)).map { case (k, v) => (k, ((n1, n2), v)) } }


    SparkEnineLeet(spark.broadcast(nod), spark.broadcast(ess), spark.broadcast(cps), newChanges, newCheckpoints, ring)
  }

  val partitioner = new HashPartitioner(partitions)

  case class SparkEnineLeet[N: CT, K: CT, V: CT](ns: Broadcast[Set[N]],
                                                 es: Broadcast[Set[(N, N)]],
                                                 cp: Broadcast[Set[N]],
                                                 changes: RDD[(K, ((N, N), V))],
                                                 checkps: RDD[(K, (N, V))],
                                                 ring: Group[V]) extends Pairleet[N, K, V]() with Serializable {

    override def g(): Group[V] = ring

    implicit def broadcase2value[T](x: Broadcast[T]): T = x.value

    override def edges(): Iterator[(N, N)] = es.toIterator

    override def nodes(): Iterator[N] = ns.toIterator

    override def size(): Long = ns.size

    override def topology(): Leet[N, Map[N, Int]] = {
      val newCheckps = spark.parallelize(cp.toSeq, partitions).map { n => (n, (n, 1)) }
      val newChanges = spark.parallelize(es.toSeq, partitions).flatMap { case (n1, n2) => Seq((n1, ((n1, n2), -1)), (n2, ((n1, n2), 1))) }

      SparkEnineLeet(ns, es, cp, newChanges, newCheckps, aInteger)
    }

    def collapse(partitioner: Partitioner): SparkEnineLeet[N, K, V] = {
      val newChanges = changes.groupByKey(partitioner).flatMap { case (k, i) =>
        merge(i.map(x => (k, x)), ring).toSeq.flatMap { case (nn, kvs) => kvs.map { case (k, v) => (k, (nn, v)) } }
      }
      val newCheckps = checkps.groupByKey(partitioner).flatMap { case (k, i) =>
        merge(i.map(x => (k, x)), ring).toSeq.flatMap { case (nn, kvs) => kvs.map { case (k, v) => (k, (nn, v)) } }
      }
      SparkEnineLeet(ns, es, cp, newChanges, newCheckps, ring)
    }

    def merge[NN, KK, VV](i: Iterable[(KK, (NN, VV))], ring: Group[VV]): Map[NN, Map[KK, VV]] = {
      val abl: GMap[NN, Map[KK, VV]] = GMap(GMap(ring))

      // TODO:  This filtering might be important in other application, CHECKKKKKKK!
      i.filter { case (k, (n, v)) => v != ring.zero() }.map { case (k, (n, v)) => Map(n -> Map(k -> v)) }.fold(abl.zero())(abl.op)
    }

    override def index(): Iterator[(N, Map[K, V])] = {
      val collapsed = collapse(partitioner)

      // Make sure that checkpoints and changes are collected after each other within this spark.
      val (changesAsync, checkpointsAsync) = spark.synchronized {
        (collapsed.changes.collectAsync(), collapsed.checkps.collectAsync())
      }

      val collectChanges = merge(Await.result(changesAsync, Duration.Inf), ring)
      val collectCheckps = merge(Await.result(checkpointsAsync, Duration.Inf), ring)

      Topleets.index(ns, es, cp, collectCheckps, collectChanges, aMap[K, V](ring))
    }

    override def diff(merges: Boolean): Leet[N, Map[K, V]] = {
      val ins = Topleets.incoming(ns, es)
      val out = Topleets.outgoing(ns, es)

      def filter(n: N): Boolean = merges || (ins(n).size == 1)

      val i = changes.flatMap {
        case (k1, ((n1, n2), v1)) if filter(n2) => ins(n2).map { in => (k1, ((in, n2), v1)) }
        case _ => Seq()
      }

      val o = changes.flatMap {
        case (k1, ((n1, n2), v1)) if filter(n2) => out(n2).map { ou => (k1, ((n2, ou), ring.inv(v1))) }
        case _ => Seq()
      }

      val c = changes.collect { case (k1, ((n1, n2), v1)) if filter(n2) && cp(n2) => (k1, (n2, v1)) }

      SparkEnineLeet(ns, es, cp, i ++ o, c, ring)
    }

    override def tmapIx[K2: CT, V2: CT](f: (K, V) => Map[K2, V2], dynamics: Dynamics)(implicit gv2: Group[V2]): Leet[N, Map[K2, V2]] = {
      val collapsed = collapse(partitioner)

      val newChanges = collapsed.changes.zipPartitions(collapsed.checkps, true) { case (changes, checkps) =>
        lazy val mchanges = merge(changes.toIterable, ring)
        lazy val mcheckps = merge(checkps.toIterable, ring)

        val ins = Topleets.incoming(ns, es)
        val out = Topleets.outgoing(ns, es)

        val changein = for ((n, ix) <- Topleets.index(ns, es, cp, mcheckps, mchanges, aMap[K, V](ring));
                            in <- ins(n).toSeq; (k, _) <- mchanges.getOrElse((in, n), Map()).toSeq if ix.isDefinedAt(k); (k2, v2) <- f(k, ix(k)).toSeq if v2 != gv2.zero())
          yield (k2, ((in, n), v2))

        val changeout = for ((n, ix) <- Topleets.index(ns, es, cp, mcheckps, mchanges, aMap[K, V](ring));
                             ou <- out(n).toSeq; (k, _) <- mchanges.getOrElse((n, ou), Map()).toSeq if ix.isDefinedAt(k); (k2, v2) <- f(k, ix(k)).toSeq if v2 != gv2.zero())
          yield (k2, ((n, ou), gv2.inv(v2)))
        changein ++ changeout
      }

      val newCheckps = collapsed.checkps.flatMap { case (k, (n, v)) => f(k, v).map { case (k2, v2) => (k2, (n, v2)) } }

      SparkEnineLeet(ns, es, cp, newChanges, newCheckps, gv2)
    }

    override def tmapIx2[K2: CT, V2: CT](f: (K, V, V) => Map[K2, V2], dynamics: Dynamics)(implicit gv2: Group[V2]): Leet[N, Map[K2, V2]] = {
      val collapsed = collapse(partitioner)

      val newChanges = collapsed.changes.zipPartitions(collapsed.checkps, true) { case (changes, checkps) =>
        lazy val mchanges = merge(changes.toIterable, ring)
        lazy val mcheckps = merge(checkps.toIterable, ring)

        val ins = Topleets.incoming(ns, es)

        for ((n, ix) <- Topleets.index(ns, es, cp, mcheckps, mchanges, aMap[K, V](ring));
             in <- ins(n).toSeq; (k, cv) <- mchanges.getOrElse((in, n), Map()).toSeq; (k2, v2) <- f(k, ix.getOrElse(k, ring.zero()), cv).toSeq if v2 != gv2.zero())
          yield (k2, ((in, n), v2))
      }

      val newCheckps = collapsed.checkps.flatMap { case (k, (n, v)) => f(k, v, v).map { case (k2, v2) => (k2, (n, v2)) } }

      SparkEnineLeet(ns, es, cp, newChanges, newCheckps, gv2)
    }


    override def tmapHom[K2: CT, V2: CT](f: (K, V) => Map[K2, V2], dynamics: Dynamics)(implicit gv2: Group[V2]): Leet[N, Map[K2, V2]] = {
      dynamics match {
        case Dynamics.Linear =>
          val newChanges = changes.flatMap { case (k, ((n1, n2), v)) => f(k, v).map { case (k2, v2) => (k2, ((n1, n2), v2)) } }
          val newCheckpoints = checkps.flatMap { case (k, (n, v)) => f(k, v).map { case (k2, v2) => (k2, (n, v2)) } }
          SparkEnineLeet(ns, es, cp, newChanges, newCheckpoints, gv2)
        case Dynamics.Collapse =>
          val collapsed = collapse(partitioner)
          val newChanges = collapsed.changes.flatMap { case (k, ((n1, n2), v)) => f(k, v).map { case (k2, v2) => (k2, ((n1, n2), v2)) } }
          val newCheckpoints = collapsed.checkps.flatMap { case (k, (n, v)) => f(k, v).map { case (k2, v2) => (k2, (n, v2)) } }
          SparkEnineLeet(ns, es, cp, newChanges, newCheckpoints, gv2)
      }
    }

    override def tadd(l: Leet[N, Map[K, V]]): Leet[N, Map[K, V]] = {
      val l1 = this
      val l2 = l.asInstanceOf[SparkEnineLeet[N, K, V]]

      val newCheckps = l1.checkps ++ l2.checkps
      val newChanges = l1.changes ++ l2.changes

      SparkEnineLeet(ns, es, cp, newChanges, newCheckps, ring)
    }

    override def tcartesian[K2: CT](l: Leet[N, Map[K2, V]]): Leet[N, Map[(K, K2), V]] = {
      val l1 = this.collapse(new HashPartitioner(Math.sqrt(partitions).toInt))
      val l2 = l.asInstanceOf[SparkEnineLeet[N, K2, V]].collapse(new HashPartitioner(Math.sqrt(partitions).toInt))

      val a = l1.changes.glom().zip(l1.checkps.glom())
      val b = l2.changes.glom().zip(l2.checkps.glom())

      val newChanges = a.cartesian(b).flatMap { case ((changes1, checkps1), (changes2, checkps2)) =>
        lazy val mchanges1 = merge(changes1.toIterable, ring)
        lazy val mcheckps1 = merge(checkps1.toIterable, ring)

        lazy val mchanges2 = merge(changes2.toIterable, l2.ring)
        lazy val mcheckps2 = merge(checkps2.toIterable, l2.ring)

        def xindex() = Topleets.index(ns, es, cp, mcheckps1, mchanges1, aMap[K, V](ring))

        def yindex() = Topleets.index(ns, es, cp, mcheckps2, mchanges2, aMap[K2, V](l2.ring))

        val ins = Topleets.incoming(ns, es)
        val out = Topleets.outgoing(ns, es)

        def left = (for ((n, ix) <- xindex(); in <- ins(n).toSeq; (k2, v2) <- mchanges2.getOrElse((in, n), Map())) yield
          ix.map { case (k1, v1) => ((k1, k2), ((in, n), ring.mop(v1, v2))) }).flatten

        def right = (for ((n, ix) <- yindex(); in <- ins(n).toSeq; (k1, v1) <- mchanges1.getOrElse((in, n), Map())) yield
          ix.map { case (k2, v2) => ((k1, k2), ((in, n), ring.mop(v1, v2))) }).flatten

        def inner = for (n <- ns.toIterator; in <- ins(n).toSeq;
                         (k1, v1) <- mchanges1.getOrElse((in, n), Map());
                         (k2, v2) <- mchanges2.getOrElse((in, n), Map())) yield
          ((k1, k2), ((in, n), ring.minv(ring.mop(v1, v2))))

        left ++ right ++ inner
      }

      val newCheckps = l1.checkps.glom().cartesian(l2.checkps.glom()).flatMap { case (checkps1, checkps2) =>
        lazy val mcheckps1 = merge(checkps1.toIterable, ring)
        lazy val mcheckps2 = merge(checkps2.toIterable, ring)

        for (n <- cp.toIterator if mcheckps1.isDefinedAt(n) && mcheckps2.isDefinedAt(n); (k1, v1) <- mcheckps1(n).toSeq; (k2, v2) <- mcheckps2(n).toSeq)
          yield ((k1, k2), (n, ring.mop(v1, v2)))
      }

      SparkEnineLeet(ns, es, cp, newChanges, newCheckps, ring)
    }

    override def tmap[K2: CT](f: K => K2, dynamics: Dynamics): Leet[N, Map[K2, V]] = dynamics match {
      case Dynamics.Linear =>
        val newChanges = changes.map { case (k, (nn, v)) => (f(k), (nn, v)) }
        val newCheckps = checkps.map { case (k, (nn, v)) => (f(k), (nn, v)) }

        SparkEnineLeet(ns, es, cp, newChanges, newCheckps, ring)
      case _ =>
        val newChanges = changes.groupByKey(partitioner).flatMap { case (k, i) =>
          val fk = f(k)
          merge(i.map(x => (fk, x)), ring).toSeq.flatMap { case (nn, kvs) => kvs.map { case (k, v) => (k, (nn, v)) } }
        }
        val newCheckps = checkps.groupByKey(partitioner).flatMap { case (k, i) =>
          val fk = f(k)
          merge(i.map(x => (fk, x)), ring).toSeq.flatMap { case (nn, kvs) => kvs.map { case (k, v) => (k, (nn, v)) } }
        }
        SparkEnineLeet(ns, es, cp, newChanges, newCheckps, ring)
    }

    override def tfilter(f: K => Boolean): Leet[N, Map[K, V]] = {
      val newChanges = changes.filter { case (k, (nn, v)) => f(k) }
      val newCheckps = checkps.filter { case (k, (nn, v)) => f(k) }

      SparkEnineLeet(ns, es, cp, newChanges, newCheckps, ring)
    }
  }

}
