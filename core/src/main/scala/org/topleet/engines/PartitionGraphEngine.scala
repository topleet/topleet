package org.topleet.engines

import org.topleet.Dynamics.Dynamics
import org.topleet.groups.Group
import org.topleet.{Engine, Leet, Pairleet, Topleets}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.reflect.{ClassTag => CT}


/**
 * An engine partitioning the background graph into connected graph components of equal size. It delegating the handling of core operations
 * on the partitions to another engine.
 *
 * This generic engine can be used if the background graph is getting to big for certain engines
 * or if the processing of an engine should be executed in parallel.
 * @param delegate The engine executing the processing on the partitions.
 * @param components The number of partitions.
 * @param e The number of overlapping hops (as every diff operation call requires data in a neighbourhood of 1 hop).
 */
case class PartitionGraphEngine(@transient delegate: Engine, components: Int, e: Int) extends Engine {
  override def create[N: CT, K: CT, V: CT](nodes: () => Set[N], inEdges: N => Set[N], values: N => Map[K, V])(implicit ring: Group[V]): Leet[N, Map[K, V]] = {
    val ns = nodes()
    val es = ns.flatMap(n2 => inEdges(n2).map(n1 => (n1, n2)))

    val css = Topleets.cut(Topleets.tsCCSeq(es, ns), components).map(_.toSet)

    val partitions = css.map { cc =>
      // Append an depth of e incoming edges to remain correct on e diff operations.
      val all = (0 to e).foldLeft(cc) { case (cc, _) => cc ++ cc.flatMap(inEdges) }
      val leet = delegate.create(() => all, inEdges.andThen(x => x.filter(all)), values)

      (cc, all, leet)
    }

    new PartitionGraphLeet(0, e, partitions.toSeq)
  }

  class PartitionGraphLeet[N1: CT, K1: CT, V1: CT](val appliedDiffs: Int, val e: Int, @transient val nested: Seq[(Set[N1], Set[N1], Leet[N1, Map[K1, V1]])]) extends Pairleet[N1, K1, V1] {

    import org.topleet.libs.Natives._

    override def g(): Group[V1] = nested.head._3.g()

    override def edges(): Iterator[(N1, N1)] = nested.toIterator.flatMap { case (cc, _, leet) =>
      leet.edges().filter { case (n1, n2) => cc(n2) }
    }

    override def nodes(): Iterator[N1] = nested.toIterator.flatMap { case (cc, _, _) => cc }

    override def size(): Long = nodes().size

    override def topology(): Leet[N1, Map[N1, Int]] =
      new PartitionGraphLeet(appliedDiffs, e, nested.map { case (cc, all, leet) => (cc, all, leet.topology()) })

  // TODO: Turn of parallel processing by flat in configurations.
    override def index(): Iterator[(N1, Map[K1, V1])] = {

//      val futures = (for((cc, all, leet) <- nested) yield {
//        // Super ugly. Fail! Needed to make spark preserve ordering of
//        Thread.sleep(100)
//        Future[Iterator[(N1, Map[K1, V1])]] {
//          leet.index().filter { case (n, _) => cc(n) }
//        }
//      }).toSet

      var futures: Set[Future[Iterator[(N1, Map[K1, V1])]]] = nested
        .map { case (cc, all, leet) =>
          Future[Iterator[(N1, Map[K1, V1])]] {
            leet.index().filter { case (n, _) => cc(n) }
          }
        }.toSet


      Iterator.fill(futures.size)(() => Unit).flatMap { x =>
        // TODO: This is no nice.
        //val f = Future.firstCompletedOf(futures)

        while (!futures.exists(_.isCompleted))
          Thread.sleep(1000)

        val f = futures.find(_.isCompleted).get

        futures = futures - f
        f.value.get.get
      }
    }

    //      nested.toIterator.flatMap {
    //          // TODO: Fix this for
    //      case (cc, _, leet) => leet.index().filter { case (n, _) => cc(n) }
    //    }

    override def diff(merges: Boolean): Leet[N1, Map[K1, V1]] = {
      if (appliedDiffs < e)
        new PartitionGraphLeet(appliedDiffs + 1, e, nested.map { case (cc, all, leet) => (cc, all, leet.diff(merges)) })
      else throw new Exception("Add more e to diff this")
    }

    override def tmapIx[K2: CT, V2: CT](f: (K1, V1) => Map[K2, V2], dynamics: Dynamics)(implicit gv2: Group[V2]): Leet[N1, Map[K2, V2]] =
      new PartitionGraphLeet(appliedDiffs, e, nested.map { case (cc, all, leet) => (cc, all, leet.tmapIx(f, dynamics)) })

    override def tmapIx2[K2: CT, V2: CT](f: (K1, V1, V1) => Map[K2, V2], dynamics: Dynamics)(implicit gv2: Group[V2]): Leet[N1, Map[K2, V2]] =
      new PartitionGraphLeet(appliedDiffs, e, nested.map { case (cc, all, leet) => (cc, all, leet.tmapIx2(f, dynamics)) })

    override def tmapHom[K2: CT, V2: CT](f: (K1, V1) => Map[K2, V2], dynamics: Dynamics)(implicit gv2: Group[V2]): Leet[N1, Map[K2, V2]] =
      new PartitionGraphLeet(appliedDiffs, e, nested.map { case (cc, all, leet) => (cc, all, leet.tmapHom(f, dynamics)) })

    override def tadd(l: Leet[N1, Map[K1, V1]]): Leet[N1, Map[K1, V1]] = {
      val other = l.asInstanceOf[PartitionGraphLeet[N1, K1, V1]]
      val out = nested.zip(other.nested).map { case ((cc1, all1, leet1), (cc2, all2, leet2)) if cc1 == cc2 && all1 == all2 => (cc1, all1, leet1.tadd(leet2)) }
      new PartitionGraphLeet(math.max(appliedDiffs, other.appliedDiffs), e, out)
    }

    override def tcartesian[K2: CT](l: Leet[N1, Map[K2, V1]]): Leet[N1, Map[(K1, K2), V1]] = {
      val other = l.asInstanceOf[PartitionGraphLeet[N1, K2, V1]]
      val out = nested.zip(other.nested).map { case ((cc1, all1, leet1), (cc2, all2, leet2)) if cc1 == cc2 && all1 == all2 => (cc1, all1, leet1.tcartesian(leet2)) }
      new PartitionGraphLeet(math.max(appliedDiffs, other.appliedDiffs), e, out)
    }

    override def tmap[K2: CT](f: K1 => K2, dynamics: Dynamics): Leet[N1, Map[K2, V1]] =
      new PartitionGraphLeet(appliedDiffs, e, nested.map { case (cc, all, leet) => (cc, all, leet.tmap(f, dynamics)) })


    override def tfilter(f: K1 => Boolean): Leet[N1, Map[K1, V1]] =
      new PartitionGraphLeet(appliedDiffs, e, nested.map { case (cc, all, leet) => (cc, all, leet.tfilter(f)) })
  }

}
