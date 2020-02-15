package org.topleet.libs

import org.topleet.groups._
import org.topleet.{Dynamics, Leet, Pairleet}
import scala.reflect.{ClassTag => CT}

object Natives extends Serializable {

  type Bag[K] = Map[K, Int]

  type Single[K] = Bag[K]

  implicit val aInteger: Group[Int] = GInteger()

  implicit val aDouble: Group[Double] = GDouble()

  implicit def aMap[K, V](implicit g: Group[V]): GMap[K, V] = GMap[K, V](g)

  implicit def aTuple[V1, V2](implicit v1: Group[V1], v2: Group[V2]): GTuple[V1, V2] = GTuple(v1, v2)

  implicit def a3Tuple[V1, V2, V3](implicit v1: Group[V1], v2: Group[V2], v3: Group[V3]): GTuple3[V1, V2, V3] = GTuple3(v1, v2, v3)

  def single[V](v: V): Single[V] = bag(Seq(v))

  def zero[K, V]: Map[K, V] = Map[K, V]()

  def bag[V](es: Iterable[V]): Bag[V] = es.map(e => Map(e -> 1)).fold(aMap[V, Int].zero())(aMap[V, Int].op)

  implicit def pairleet[N, K, V](l: Leet[N, Map[K, V]]): Pairleet[N, K, V] = l.asInstanceOf[Pairleet[N, K, V]]

  implicit class BaseProvide[N1: CT, K1: CT, V1: CT](@transient l: Leet[N1, Map[K1, V1]]) extends Serializable {

    def mapValues[V2: CT](f: V1 => V2)(implicit gv2: Group[V2]): Leet[N1, Map[K1, V2]] = l.tmapIx { case (k, v) => Map(k -> f(v)) }

    def parents(): Leet[N1, Bag[(N1, N1)]] = {
      val td = l.topology().diff()

      td.removed().tcartesian(td.added())
    }

    def escape(): Leet[N1, Single[Map[K1, V1]]] = l
      .groupBy(_ => null.asInstanceOf[Unit])
      .tmapIx { case (k, v) => single(v) }

    def map[K2: CT](f: K1 => K2, dynamics: Dynamics.Dynamics = Dynamics.Collapse): Leet[N1, Map[K2, V1]] =
      l.tmap(f, dynamics)

    def filter(f: K1 => Boolean, dynamics: Dynamics.Dynamics = Dynamics.Linear): Leet[N1, Map[K1, V1]] = {
      l.map({ k => (k, f(k)) }, Dynamics.Memoization)
        .tfilter { case (_, condition) => condition }
        .tmap({ case (k, _) => k }, Dynamics.Linear)
    }

    def inv(): Leet[N1, Map[K1, V1]] = {
      implicit val abl: Group[V1] = l.g()
      l.mapValuesHom(x => abl.inv(x), dynamics = Dynamics.Linear)
    }

    def flatMap[K2: CT](f: K1 => Map[K2, V1], dynamics: Dynamics.Dynamics = Dynamics.Collapse): Leet[N1, Map[K2, V1]] =
      l.map(f, dynamics).flatten()

    def collect[K2: CT](f: PartialFunction[K1, K2], dynamics: Dynamics.Dynamics = Dynamics.Collapse): Leet[N1, Map[K2, V1]] = {
      def liftedf(x: K1): Option[K2] = f.lift(x)

      l.map(liftedf, dynamics).filter(_.isDefined).map { case Some(k2) => k2 }
    }


    def filterValues(f: V1 => Boolean): Leet[N1, Map[K1, V1]] = {
      implicit val abl: Group[V1] = l.g()
      l.tmapIx { case (k, v) => if (f(v)) Map(k -> v) else Map() }
    }

    def mapValuesHom[V2: CT](f: V1 => V2, dynamics: Dynamics.Dynamics = Dynamics.Collapse)(implicit gv2: Group[V2]): Leet[N1, Map[K1, V2]] = l.tmapHom({ case (k, v) => Map(k -> f(v)) }, dynamics)

    def toBag: Leet[N1, Map[(K1, V1), Int]] = l.tmapIx { case (k, v) => Map((k, v) -> 1) }

    def ++[K >: K1 : CT, K2 <: K : CT](l2: Leet[N1, Map[K2, V1]]): Leet[N1, Map[K, V1]] = {
      implicit val av1: Group[V1] = l.g()
      l.tmapHom { case (k, v) => Map(k.asInstanceOf[K] -> v) }.tadd(l2.tmapHom { case (k, v) => Map(k.asInstanceOf[K] -> v) })
    }

    def join(@transient that: Leet[N1, Map[K1, V1]]): Leet[N1, Map[K1, V1]] = l.tjoin(that)

    def cogroup[V2: CT](@transient that: Leet[N1, Map[K1, V2]]): Leet[N1, Map[K1, (V1, V2)]] = {
      implicit val av1: Group[V1] = l.g()
      implicit val tupleGroup: GTuple[V1, V2] = GTuple(av1, that.g())
      val thatAbl = that.g()

      val out2 = l.mapValuesHom(v => (v, thatAbl.zero()), dynamics = Dynamics.Linear)
      val out1 = that.mapValuesHom(v => (av1.zero(), v), dynamics = Dynamics.Linear)

      out1.tadd(out2)
    }

    def groupBy[G: CT](f: K1 => G): Leet[N1, Map[G, Map[K1, V1]]] = {
      implicit val av1: Group[V1] = l.g()
      l.tmapHom({ case (k, v) => Map(f(k) -> Map(k -> v)) }, dynamics = Dynamics.Linear)
    }

  }

  implicit class BagProvide[N: CT, K1: CT](@transient l1: Leet[N, Bag[K1]]) extends Serializable {

    // TODO: Make this right and add 0 entries for empty commits.
    def length(): Leet[N, Single[Int]] = l1.tmapHom { case (_, v) => Map(null.asInstanceOf[Unit] -> v) }.tmapIx {
      case (_, v) => Map(v -> 1)
    }

    def distinct(): Leet[N, Map[K1, Int]] = l1.mapValues(x => math.max(math.min(1, x), -1))

    /**
     * Just returns the positive elements of the bag.
     */
    def added(): Leet[N, Map[K1, Int]] = l1.mapValues(x => math.max(0, x))

    /**
     * Just returns the negative elements of the bag.
     */
    def removed(): Leet[N, Map[K1, Int]] = l1.mapValues(x => math.min(0, x))

    // def cartesian[K2: CT](that: Leet[N, Bag[K2]]): Leet[N, Bag[(K1, K2)]] = l.product(that).mapValues { case (l, r) => l * r }
    //    def cartesian[K2: CT](l2: Leet[N, Bag[K2]]): Leet[N, Bag[(K1, K2)]] =
    //      l1.cartesian(l2)
  }

  implicit class BagIntProvide[N: CT](@transient l: Leet[N, Bag[Int]]) extends Serializable {

    def sum(): Leet[N, Single[Int]] = l
      .tmapHom { case (k, v) => Map(null.asInstanceOf[Unit] -> k * v) }
      .tmapIx { case (_, v) => Map(v -> 1) }
  }

  // Do not use vectors in change due to rounding errors.
  //  implicit class GroupedVectorProvide[N: CT, K: CT, K2:CT](@transient l: Leet[N, Map[K, Map[K2, Double]]]) extends Serializable {
  //
  //    def length(): Leet[N, Map[K, Double]] = l.tmapHom({ case (k, v) => Map(k -> v.toSeq.map { case (e, c) => c }.sum) }, dynamics = Dynamics.Linear)
  //
  //  }

  implicit class GroupedBagIntProvide[N: CT, K: CT](@transient l: Leet[N, Map[K, Bag[Int]]]) extends Serializable {

    def length(): Leet[N, Map[K, Int]] = l.tmapHom({ case (k, v) => Map(k -> v.toSeq.map { case (e, c) => c }.sum) }, dynamics = Dynamics.Linear)

    def sum(): Leet[N, Map[K, Int]] = l.tmapHom({ case (k, v) => Map(k -> v.toSeq.map { case (e, c) => e * c }.sum) }, dynamics = Dynamics.Linear)

    def avg(): Leet[N, Map[K, Double]] = {
      l.tmapHom({ case (k, v) =>
        Map(k -> (v.toSeq.map { case (e, c) => e * c }.sum, v.toSeq.map { case (_, c) => c }.sum))
      }, dynamics = Dynamics.Linear).tmapIx {
        case (k, (sum, count)) if count != 0 => Map(k -> sum.toDouble / count.toDouble)
        case (k, (_, count)) if count == 0 => Map()
      }
    }
  }

  implicit class GroupedBagProvide[N: CT, K1: CT, K2: CT](@transient l: Leet[N, Map[K1, Bag[K2]]]) extends Serializable {

    def length(): Leet[N, Map[K1, Int]] = l.tmapHom({ case (k, v) => Map(k -> v.toSeq.map { case (e, c) => c }.sum) }, dynamics = Dynamics.Linear)
  }

  // TODO: THIS IS IMPORTANT (UNTERSTAND!):
  implicit class ValueMapProvide[N: CT, K1: CT, K2: CT, V: CT](@transient l: Leet[N, Map[K1, Map[K2, V]]]) extends Serializable {
    implicit val thatGroup: Group[V] = l.g().asInstanceOf[GMap[K2, V]].gv

    def ungroup(): Leet[N, Map[(K1, K2), V]] = l.tmapHom({ case (k1, vs) => vs.map { case (k2, v) => (k1, k2) -> v } }, dynamics = Dynamics.Linear)

    // Not really neaded.
    //    def nestedFilter(f: K2 => Boolean): Pairleet[N, K1, Map[K2, V]] = l
    //      .ungroup()
    //      .tfilter { case (k1, k2) => f(k2) }
    //      .group()
    //
    //    def nestedMap[K3: CT](f: K2 => K3): Pairleet[N, K1, Map[K3, V]] = l
    //      .ungroup()
    //      .map { case (k1, k2) => (k1, f(k2)) }
    //      .group()


    //def filter2(f: K2 => Boolean): Leet[N, K1, Map[K2, V]] = l.mapValuesHom { case (k, v) => Map(f(k) -> v) }
  }

  implicit class KeyPairProvide[N: CT, K1: CT, K2: CT, V: CT](@transient l: Leet[N, Map[(K1, K2), V]]) extends Serializable {

    implicit val thatGroup: Group[Map[K2, V]] = GMap(l.g())

    def group(): Leet[N, Map[K1, Map[K2, V]]] = l.tmapHom({ case ((k1, k2), v) => Map(k1 -> Map(k2 -> v)) }, dynamics = Dynamics.Linear)
  }

  implicit class KeyBagProvide[N: CT, K1: CT, V1: CT](@transient l: Leet[N, Map[Map[K1, V1], V1]]) extends Serializable {
    implicit val abl: Group[V1] = l.g()

    def flatten(): Leet[N, Map[K1, V1]] = l.tmapHom({ case (kvs, v1) => kvs.map { case (k, vv1) => k -> abl.mop(v1, vv1) } }, dynamics = Dynamics.Linear)

  }

  //  implicit class NestedCartesianProvider[N: CT, K1: CT, K2: CT, K3: CT, V1: CT](@transient l: Leet[N, Map[K1, (Map[K2, V1], Map[K3, V1])]]) extends Serializable {
  //
  //    implicit val abl: Group[V1] = l.g().asInstanceOf[GTuple[Map[K2, V1], Map[K3, V1]]].g1.asInstanceOf[GMap[K1, V1]].gv
  //
  //    def nestedCartesian(): Leet[N, Map[K1, Map[(K2, K3), V1]]] =
  //      l.tmapIx2 { case (k1, (x, y), (dx, dy)) =>
  //        // Incremental cartesian product.
  //        val mabl = aMap[(K2, K3), V1]
  //        val lll = for ((xk, xv) <- x.toSeq; (dyk, dyv) <- dy) yield Map((xk, dyk) -> abl.mop(xv, dyv))
  //        val rrr = for ((yk, yv) <- y.toSeq; (dxk, dxv) <- dx) yield Map((dxk, yk) -> abl.mop(dxv, yv))
  //        val mmm = for ((dyk, dyv) <- dy.toSeq; (dxk, dxv) <- dx) yield Map((dxk, dyk) -> abl.inv(abl.mop(dxv, dyv)))
  //
  //        Map(k1 -> (lll ++ rrr ++ mmm).fold(mabl.zero())(mabl.op))
  //      }
  //
  //  }
}
