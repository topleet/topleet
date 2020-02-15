package org.topleet.groups

/**
 * Field for Map containing an issue with the multiplicative zero as this might imply every possible value
 * to be the multiplicative zero of V.
 */
case class GMap[K, V](gv: Group[V]) extends Group[Map[K, V]] {
  override def zero(): Map[K, V] = Map()

  override def inv(x: Map[K, V]): Map[K, V] =
    x.map { case (k, v) => (k, gv.inv(v)) }

  override def op(a: Map[K, V], b: Map[K, V]): Map[K, V] = (a, b) match {
    case (smaller, bigger) if smaller.size < bigger.size => op(bigger, smaller)
    case (bigger, smaller) => smaller.foldLeft(bigger) {
      case (m, (k, v)) if m.isDefinedAt(k) && gv.op(m(k), v) != gv.zero => m.updated(k, gv.op(m(k), v))
      case (m, (k, _)) if m.isDefinedAt(k) => m - k
      case (m, (k, v)) => m.updated(k, v)
    }
  }

  override def mop(a: Map[K, V], b: Map[K, V]): Map[K, V] = a.keySet.intersect(b.keySet)
    .map { k => k -> gv.mop(a(k), b(k)) }.toMap

  override def minv(a: Map[K, V]): Map[K, V] =
    a.map { case (k, v) => (k, gv.minv(v)) }

  // TODO: Pain but seems to be doable if dimensions are hypothetically fixed.
  override def mzero(): Map[K, V] = ???
}
