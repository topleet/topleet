package org.topleet.engines

import org.topleet.Dynamics.Dynamics
import org.topleet.groups.Group
import org.topleet.{Engine, Leet, Pairleet}

import scala.reflect.{ClassTag => CT}

/**
  * Base Engine for delegating core functionality a set of engines. TODO: Refactor to intercept every core operations.
  */
abstract class DelegateEngine(@transient engines: Seq[Engine]) extends Engine {

  import org.topleet.libs.Natives._

  override def create[N: CT, K: CT, V: CT](nodes: () => Set[N], inEdges: N => Set[N], values: N => Map[K, V])(implicit ring: Group[V]): Leet[N, Map[K, V]] = {
    new DELLeet(after("create", engines.map(e => e.create(nodes, inEdges, values))))
  }


  def dmode(name: String, dynamics: Dynamics): Dynamics = dynamics

  def beforeUnary[N2: CT, K2: CT, V2: CT](name: String, ls: Seq[Leet[N2, Map[K2, V2]]]): Seq[Leet[N2, Map[K2, V2]]] = ls

  def beforeBinary[N2: CT, K2: CT, V2: CT, K3: CT, V3: CT](name: String, ls: Seq[(Leet[N2, Map[K2, V2]], Leet[N2, Map[K3, V3]])]): Seq[(Leet[N2, Map[K2, V2]], Leet[N2, Map[K3, V3]])] = ls

  def after[N2: CT, K2: CT, V2: CT](name: String, ls: Seq[Leet[N2, Map[K2, V2]]]): Seq[Leet[N2, Map[K2, V2]]] = ls

  def checkIndex[N1: CT,K1: CT,V1: CT](leet: DELLeet[N1, K1, V1]): Iterator[(N1, Map[K1, V1])] = {
    val results = leet.nested.map(_.index())
    results.head
  }

  class DELLeet[N1: CT, K1: CT, V1: CT](@transient val nested: Seq[Leet[N1, Map[K1, V1]]]) extends Pairleet[N1, K1, V1] {

    override def g(): Group[V1] =
      nested.head.g()


    override def edges(): Iterator[(N1, N1)] =
      nested.head.edges()

    override def nodes(): Iterator[N1] =
      nested.head.nodes()

    override def size(): Long = nested.head.size()

    override def index(): Iterator[(N1, Map[K1, V1])] = checkIndex(this)

    override def diff(merges: Boolean): Leet[N1, Map[K1, V1]] = {
      val id = "diff"
      val out = beforeUnary(id, nested).map(l => l.diff(merges))
      new DELLeet(after(id, out))
    }

    override def topology(): Leet[N1, Map[N1, Int]] = {
      val id = "topology"
      val out = beforeUnary(id, nested).map(l => l.topology())
      new DELLeet(after(id, out))
    }

    override def tmapIx[K2: CT, V2: CT](f: (K1, V1) => Map[K2, V2], dynamics: Dynamics)(implicit gv2: Group[V2]): Leet[N1, Map[K2, V2]] = {
      val id = "tmapIx" + " (" + dynamics.toString() + ")"
      val jdynamics = dmode(id, dynamics)
      val out = beforeUnary(id, nested).map(l => l.tmapIx(f, jdynamics))
      new DELLeet(after(id, out))
    }

    override def tmapIx2[K2: CT, V2: CT](f: (K1, V1, V1) => Map[K2, V2], dynamics: Dynamics)(implicit gv2: Group[V2]): Leet[N1, Map[K2, V2]] = {
      val id = "tmapIx2" + " (" + dynamics.toString() + ")"
      val jdynamics = dmode(id, dynamics)
      val out = beforeUnary(id, nested).map(l => l.tmapIx2(f, jdynamics))
      new DELLeet(after(id, out))
    }

    override def tmapHom[K2: CT, V2: CT](f: (K1, V1) => Map[K2, V2], dynamics: Dynamics)(implicit gv2: Group[V2]): Leet[N1, Map[K2, V2]] = {
      val id = "tmapHom" + " (" + dynamics.toString() + ")"
      val jdynamics = dmode(id, dynamics)
      val out = beforeUnary(id, nested).map(l => l.tmapHom(f, jdynamics))
      new DELLeet(after(id, out))
    }

    override def tadd(l: Leet[N1, Map[K1, V1]]): Leet[N1, Map[K1, V1]] = {
      val id = "add"
      val other = l.asInstanceOf[DELLeet[N1, K1, V1]]
      val out = beforeBinary(id, nested.zip(other.nested)).map { case (l, r) => l.tadd(r) }
      new DELLeet(after(id, out))
    }

    override def tcartesian[K2: CT](l: Leet[N1, Map[K2, V1]]): Leet[N1, Map[(K1, K2), V1]] = {
      val id = "cartesian"
      val other = l.asInstanceOf[DELLeet[N1, K2, V1]]
      val out = beforeBinary(id, nested.zip(other.nested)).map { case (l, r) => l.tcartesian(r) }
      new DELLeet(after(id, out))
    }

    override def tmap[K2: CT](f: K1 => K2, dynamics: Dynamics): Leet[N1, Map[K2, V1]] = {
      val id = "map" + " (" + dynamics.toString() + ")"
      val jdynamics = dmode(id, dynamics)
      val out = beforeUnary(id, nested).map(l => l.tmap(f, jdynamics))
      new DELLeet(after(id, out))
    }

    override def tfilter(f: K1 => Boolean): Leet[N1, Map[K1, V1]] = {
      val id = "filter"
      val out = beforeUnary(id, nested).map(l => l.tfilter(f))
      new DELLeet(after(id, out))
    }
  }

}

