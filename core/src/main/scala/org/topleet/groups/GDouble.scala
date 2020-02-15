package org.topleet.groups

/**
 * Field for Double.
 */
case class GDouble() extends Group[Double] {
  override def zero(): Double = 0

  override def op(a: Double, b: Double): Double = a + b

  override def inv(a: Double): Double = -a

  override def mop(a: Double, b: Double): Double = a * b

  override def minv(a: Double): Double = a * -1

  override def mzero(): Double = 1.0
}
