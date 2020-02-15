package org.topleet.groups

/**
 * The group trait implementing a Field.
 *
 * @tparam V
 */
trait Group[V] {

  /**
   * The additive zero element.
   */
  def zero(): V

  /**
   * The multiplicative zero element.
   *
   * @return
   */
  def mzero(): V

  /**
   * The additive group operator.
   */
  def op(a: V, b: V): V

  /**
   * The multiplicative group operator.
   */
  def mop(a: V, b: V): V

  /**
   * The additive inverse operator.
   */
  def inv(a: V): V

  /**
   * The multiplicative inverse operator.
   */
  def minv(a: V): V

  /**
   * The additive diff operator.
   */
  def diff(a: V, b: V): V = op(inv(a), b)
}
