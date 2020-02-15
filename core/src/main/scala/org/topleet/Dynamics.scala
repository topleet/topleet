package org.topleet

/**
  * Dynamics of a function applications.
  */
object Dynamics extends Enumeration {
  type Dynamics = Value
  val Linear, Collapse, Memoization = Value
}
