package org.topleet.groups

/**
 * Field for 2 Tuple.
 */
case class GTuple[V1, V2](g1: Group[V1], g2: Group[V2]) extends Group[(V1, V2)] {
  override def zero(): (V1, V2) = (g1.zero(), g2.zero())

  override def op(a: (V1, V2), b: (V1, V2)): (V1, V2) = (g1.op(a._1, b._1), g2.op(a._2, b._2))

  override def inv(a: (V1, V2)): (V1, V2) = (g1.inv(a._1), g2.inv(a._2))

  override def mop(a: (V1, V2), b: (V1, V2)): (V1, V2) = (g1.mop(a._1, b._1), g2.mop(a._2, b._2))

  override def minv(a: (V1, V2)): (V1, V2) = (g1.minv(a._1), g2.minv(a._2))

  override def mzero(): (V1, V2) = (g1.mzero(), g2.mzero())
}

/**
 * Field for 3 Tuple.
 */
case class GTuple3[V1, V2, V3](g1: Group[V1], g2: Group[V2], g3: Group[V3]) extends Group[(V1, V2, V3)] {
  override def zero(): (V1, V2, V3) = (g1.zero(), g2.zero(), g3.zero())

  override def op(a: (V1, V2, V3), b: (V1, V2, V3)): (V1, V2, V3) = (g1.op(a._1, b._1), g2.op(a._2, b._2), g3.op(a._3, b._3))

  override def inv(a: (V1, V2, V3)): (V1, V2, V3) = (g1.inv(a._1), g2.inv(a._2), g3.inv(a._3))

  override def mop(a: (V1, V2, V3), b: (V1, V2, V3)): (V1, V2, V3) = (g1.mop(a._1, b._1), g2.mop(a._2, b._2), g3.mop(a._3, b._3))

  override def minv(a: (V1, V2, V3)): (V1, V2, V3) = (g1.minv(a._1), g2.minv(a._2), g3.minv(a._3))

  override def mzero(): (V1, V2, V3) = (g1.mzero(), g2.mzero(), g3.mzero())
}
