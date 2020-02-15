package org.topleet

import org.topleet.groups.{GTuple, Group}
import org.topleet.libs.Natives.Bag

import scala.reflect.{ClassTag => CT}


/**
 * The core interface of the data structure currently limited to key-value pairs.
 * The corresponding type [[org.topleet.Leet]] serves as syntactic sugar, allowing definitions without
 * explicitly referring to the key and value:
 * {{{
 * Leet[N,Map[K,V]]
 * Leet[N,Bag[K]]
 * Leet[N,Single[K]]
 * }}}
 * Operations of type Pairleet are enabled on type Leet using an implicit conversion contained in the
 * mandatory native library [[org.topleet.libs.Natives]].
 *
 * TODO:  A tree aggregation pattern is needed in this interface for monoid homomorphisms (like min or max).
 *
 * @tparam N1 is the type of the nodes in the background graph.
 * @tparam K1 is the type of the keys of the data assigned to each node.
 * @tparam V1 is the type of the values of the data assigned to each node.
 */
abstract class Pairleet[N1: CT, K1: CT, V1: CT] extends Leet[N1, Map[K1, V1]] {

  /**
   * ==Core Interface==
   * The group used for the change representation of the Map's values V1.
   *
   * @return group for V1.
   */
  def g(): Group[V1]

  /**
   * ==Core Interface==
   * The nodes of the background graph.
   *
   * @return iterator for nodes.
   */
  def nodes(): Iterator[N1]

  /**
   * ==Core Interface==
   * The edges of the background graph.
   *
   * @return iterator for edges.
   */
  def edges(): Iterator[(N1, N1)]

  /**
   * ==Core Interface==
   * The number of nodes in the background graph.
   * @return number of nodes as long.
   */
  def size(): Long

  /**
   * ==Core Interface==
   * Constructs a new data structure with nodes assigned to a single element bags containing the corresponding node's id
   *
   * @return a data structure .
   */
  def topology(): Leet[N1, Bag[N1]]

  /**
   * ==Core Interface==
   * An '''index traversal''' providing access to each node's '''key-values entries'''. Depending on the underlying implementation,
   * the traversal may efficiently evolve an immutable Map by just replacing changed values.
   * There is no guarantee on the ordering of the iterator. Every node occurs once.
   *
   * @return an iterator with the data entries for each node.
   */
  def index(): Iterator[(N1, Map[K1, V1])]

  /**
   * ==Core Interface==
   * A '''differentiation''' of the data, according to the group for V1.
   * TODO: Note that after calling diff, non-incremental engines can be preferred. Add further static/dynamic optimization.
   *
   * @param merges Defines if nodes with more that one incoming edges are considered in the differentiation.
   * @return A data structure with aggregated incoming changes on the edges assigned to the nodes new data.
   */
  def diff(merges: Boolean = true): Leet[N1, Map[K1, V1]]

  /**
   * ==Core Interface==
   * A '''topological map''' applying a '''group homomorphism''' to the data entries of the data structure.
   * If this property of the function is not clear or has to be tested, a correspondence check engine can be used,
   * comparing incremental and non-incremental behaviour on concrete data.
   *
   * @param h The group homomorphism applied to key-value pairs. Note that zero values need to be mapped to empty Maps ('''zero-to-zero''').
   *          If zero-to-zero this is not intended, different types need to be used.
   * @param dynamics Define the preferred dynamics of the application (linear, memorization or collapse).
   * @param gv2 The group for V2 usually inferred implicitly coming from [[org.topleet.libs.Natives]].
   * @tparam K2 The new key.
   * @tparam V2 The new values.
   * @return A new data structure with h applied.
   */
  def tmapHom[K2: CT, V2: CT](h: (K1, V1) => Map[K2, V2], dynamics: Dynamics.Dynamics = Dynamics.Collapse)(implicit gv2: Group[V2]): Leet[N1, Map[K2, V2]]

  /**
   * ==Core Interface==
   * A '''topological map''' applying a function that is '''no group homomorphism ''' to the key-value pairs of the data structure.
   *
   * @param f The function applied to the key-value pairs. Note that zero values need to be mapped to empty Maps ('''zero-to-zero''').
   * @param dynamics Define the preferred dynamics of the application (linear, memorization or collapse).
   * @param gv2 The group for V2 usually inferred implicitly coming from [[org.topleet.libs.Natives]].
   * @tparam K2 The new key.
   * @tparam V2 The new values.
   * @return A new data structure with f applied.
   */
  def tmapIx[K2: CT, V2: CT](f: (K1, V1) => Map[K2, V2], dynamics: Dynamics.Dynamics = Dynamics.Collapse)(implicit gv2: Group[V2]): Leet[N1, Map[K2, V2]]

  /**
   * ==Core Interface==
   * A '''topological map''' applying a function that is '''no group homomorphism ''' to the key-value pairs of the data structure.
   * This function is an '''extended form of tmapIx''' that may operate on changes.
   *
   * @param f The function applied to the key-value-change pairs returning the new change.
   * @param dynamics Define the preferred dynamics of the application (linear, memorization or collapse).
   * @param gv2 The group for V2 usually inferred implicitly coming from [[org.topleet.libs.Natives]].
   * @tparam K2 The new key.
   * @tparam V2 The new values.
   * @return A new data structure with f applied.
   */
  def tmapIx2[K2: CT, V2: CT](f: (K1, V1, V1) => Map[K2, V2], dynamics: Dynamics.Dynamics = Dynamics.Collapse)(implicit gv2: Group[V2]): Leet[N1, Map[K2, V2]]


  /**
   * ==Core Interface==
   * The '''merge operator''' (additive group operator) of the data structure.
   * The library extensions in [[org.topleet.libs.Natives]] provide a more convenient ++ operator.
   *
   * @param l The second data structure to be merged.
   * @return The merged data structure.
   */
  def tadd(l: Leet[N1, Map[K1, V1]]): Leet[N1, Map[K1, V1]]

  /**
   * ==Core Interface==
   * The cartesian product of the key-values pairs of two data structures where the values are multiplied by the
   * multiplicative operator of the corresponding group for V1.
   *
   * @param l The second data structure.
   * @tparam K2 The key of the second data structure.
   * @return The cartesian product.
   */
  def tcartesian[K2: CT](l: Leet[N1, Map[K2, V1]]): Leet[N1, Map[(K1, K2), V1]]

  /**
   * ==Core Interface==
   * The join of the key-values pairs of two data structures where the values are multiplied by the
   * multiplicative operator of the corresponding group for V1.
   * TODO: This implementation needs to be pulled into the subclasses and implemented with respect to the particular engine.
   *
   * @param l The second data structure.
   * @return The join.
   */
  def tjoin(l: Leet[N1, Map[K1, V1]]): Leet[N1, Map[K1, V1]] = {
    implicit val av1: Group[V1] = this.g()
    implicit val tupleGroup: GTuple[V1, V1] = GTuple(av1, av1)

    val left = this.tmapHom({ case (k, v) => Map(k -> (av1.zero(), v)) }, Dynamics.Linear)
      .asInstanceOf[Pairleet[N1, K1, (V1, V1)]]
    val right = l.asInstanceOf[Pairleet[N1, K1, V1]].tmapHom({ case (k, v) => Map(k -> (v, av1.zero())) }, Dynamics.Linear)
      .asInstanceOf[Pairleet[N1, K1, (V1, V1)]]

    val merged = left.tadd(right).asInstanceOf[Pairleet[N1, K1, (V1, V1)]]

    merged.tmapIx2 { case (k1, (x, y), (dx, dy)) =>
      // Incremental cartesian product for key.
      Map(k1 -> av1.op(av1.op(av1.mop(x, dy), av1.mop(y, dx)), av1.minv(av1.mop(dx, dy))))
    }
  }

  /**
   * ==Core Interface==
   *
   * A specific '''topological map''' for changing the keys, which could be realized using tmapHom, but may exhibit more
   * efficient implementations in concrete engines.
   *
   * @param f The function mapping the keys.
   * @param dynamics Define the preferred dynamics of the application (linear, memorization or collapse).
   * @tparam K2 The key of the new data statucture.
   * @return The new data structure.
   */
  def tmap[K2: CT](f: K1 => K2, dynamics: Dynamics.Dynamics = Dynamics.Collapse): Leet[N1, Map[K2, V1]]

  /**
   * ==Core Interface==
   * A specific '''topological map''' for filtering data entries by key, which could be realized using tmapHom, but may exhibit more
   * efficient implementations in concrete engines.
   *
   * @param f The function filtering the key.
   * @return The new data structure.
   */
  def tfilter(f: K1 => Boolean): Leet[N1, Map[K1, V1]]

}