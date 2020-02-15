package org.topleet

/**
 * The base interface for the data structure.
 * See [[org.topleet.Pairleet]] for a corresponding interface (currently limited to key-value pairs for V1).
 *
 * @tparam N1 is the type of the nodes in the background graph.
 * @tparam V1 is the type of the data assigned to each node.
 */
trait Leet[N1, V1] extends Serializable
