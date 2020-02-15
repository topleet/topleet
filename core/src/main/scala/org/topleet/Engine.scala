package org.topleet

import org.topleet.groups.Group

import scala.reflect.{ClassTag => CT}

/**
 * The factory interface for any data structure implementation.
 */
trait Engine extends Serializable {

  /**
   * The constructor a data structure implemented by concrete subclasses.
   *
   * @param nodes The nodes of the background graph.
   * @param inEdges The edges of the background graph.
   * @param values The values on the nodes.
   * @param group The corresponding group.
   * @tparam N The type of the nodes.
   * @tparam K The type of the keys.
   * @tparam V The type of the values.
   * @return The new data structure.
   */
  def create[N: CT, K: CT, V: CT](nodes: () => Set[N], inEdges: N => Set[N], values: N => Map[K, V])
                                 (implicit group: Group[V]): Leet[N, Map[K, V]]
}