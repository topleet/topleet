package org.topleet.groups

/**
 * Field for Integer.
 */
case class GInteger() extends Group[Int] {

  override def zero(): Int = 0

  override def op(a: Int, b: Int): Int = a + b

  override def inv(a: Int): Int = -a

  override def mop(a: Int, b: Int): Int = a * b

  override def minv(a: Int): Int = a * -1

  override def mzero(): Int = 1
}
