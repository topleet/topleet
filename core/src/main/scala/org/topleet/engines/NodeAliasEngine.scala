package org.topleet.engines

import org.topleet.Dynamics.Dynamics
import org.topleet.groups.Group
import org.topleet.{Engine, Leet, Pairleet}

import scala.reflect.{ClassTag => CT}

/**
 * An engine mapping the node ids to Short or Int datatypes to decrease the network traffic during distribution. This still
 * needs to be evaluation.
 * @param delegate The underlying engine.
 */
case class NodeAliasEngine(delegate: Engine) extends Engine {

  import org.topleet.libs.Natives._

  override def create[N: CT, K: CT, V: CT](nodes: () => Set[N], inEdges: N => Set[N], values: N => Map[K, V])(implicit ring: Group[V]): Leet[N, Map[K, V]] = {

    val source = nodes().toSeq
    // Short identification.

    if (source.size <  65534) {

      val m = source.zip(Short.MinValue to Short.MaxValue).map { case (sn, tn) => (sn, tn.toShort) }.toMap
      val im = m.map(_.swap)

      val leet = delegate.create[Short, K, V](
        () => im.keySet,
        n => inEdges(im(n)).map(m),
        n => values(im(n)))
      return NodeAliasLeet(leet, m, im)
    }

    val m = source.zip(0 until Int.MaxValue).toMap
    val im = m.map(_.swap)

    val leet = delegate.create[Int, K, V](
      () => im.keySet,
      n => inEdges(im(n)).map(m),
      n => values(im(n)))
    NodeAliasLeet(leet, m, im)

  }

  case class NodeAliasLeet[SN: CT, TN: CT, K: CT, V: CT](leet: Leet[TN, Map[K, V]],
                                                         m: Map[SN, TN],
                                                         im: Map[TN, SN]) extends Pairleet[SN, K, V] {

    override def g(): Group[V] = leet.g()

    override def edges(): Iterator[(SN, SN)] = leet.edges().map { case (n1, n2) => (im(n1), im(n2)) }

    override def nodes(): Iterator[SN] = leet.nodes().map(im)

    override def size(): Long = leet.size()

    override def topology(): Leet[SN, Map[SN, Int]] = NodeAliasLeet(leet.topology().tmap(x => im(x)), m, im)

    override def index(): Iterator[(SN, Map[K, V])] = leet.index().map { case (tn, kvs) => (im(tn), kvs) }

    override def diff(merges: Boolean): Leet[SN, Map[K, V]] = NodeAliasLeet(leet.diff(merges), m, im)

    override def tmapIx[K2: CT, V2: CT](f: (K, V) => Map[K2, V2], dynamics: Dynamics)(implicit gv2: Group[V2]): Leet[SN, Map[K2, V2]] =
      NodeAliasLeet(leet.tmapIx(f, dynamics), m, im)

    override def tmapIx2[K2: CT, V2: CT](f: (K, V, V) => Map[K2, V2], dynamics: Dynamics)(implicit gv2: Group[V2]): Leet[SN, Map[K2, V2]] =
      NodeAliasLeet(leet.tmapIx2(f, dynamics), m, im)

    override def tmapHom[K2: CT, V2: CT](f: (K, V) => Map[K2, V2], dynamics: Dynamics)(implicit gv2: Group[V2]): Leet[SN, Map[K2, V2]] =
      NodeAliasLeet(leet.tmapHom(f, dynamics), m, im)

    override def tadd(l: Leet[SN, Map[K, V]]): Leet[SN, Map[K, V]] = {
      val l1 = this
      val l2 = l.asInstanceOf[NodeAliasLeet[SN, TN, K, V]]

      NodeAliasLeet(l1.leet.tadd(l2.leet), m, im)
    }

    override def tcartesian[K2: CT](l: Leet[SN, Map[K2, V]]): Leet[SN, Map[(K, K2), V]] = {
      val l1 = this
      val l2 = l.asInstanceOf[NodeAliasLeet[SN, TN, K2, V]]

      NodeAliasLeet(l1.leet.tcartesian(l2.leet), m, im)
    }

    override def tmap[K2: CT](f: K => K2, dynamics: Dynamics): Leet[SN, Map[K2, V]] =
      NodeAliasLeet(leet.tmap(f, dynamics), m, im)

    override def tfilter(f: K => Boolean): Leet[SN, Map[K, V]] =
      NodeAliasLeet(leet.tfilter(f), m, im)
  }

}
