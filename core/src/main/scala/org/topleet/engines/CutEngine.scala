package org.topleet.engines

import org.topleet.{Engine, Leet}

import scala.reflect.{ClassTag => CT}

/**
 * An engine for cutting the computation at a given step. Filter is used to remove all content in all following computations.
 * @param engine The underlying engine.
 * @param step The step where to cut the call.
 */
case class CutEngine(engine: Engine, step: Int) extends DelegateEngine(Seq(engine)) {
  import org.topleet.libs.Natives._

  var i = 0

  override def beforeBinary[N2: CT, K2: CT, V2: CT, K3: CT, V3: CT](name: String, ls: Seq[(Leet[N2, Map[K2, V2]], Leet[N2, Map[K3, V3]])]): Seq[(Leet[N2, Map[K2, V2]], Leet[N2, Map[K3, V3]])] = {
    i = i + 1
    if (i > step) ls.map { case (l, r) => (l.tfilter(_ => false), r.tfilter(_ => false)) }
    else ls
  }

  override def beforeUnary[N2: CT, K2: CT, V2: CT](name: String, ls: Seq[Leet[N2, Map[K2, V2]]]): Seq[Leet[N2, Map[K2, V2]]] = {
    i = i + 1
    if (i > step) ls.map { l => l.tfilter(_ => false) }
    else ls
  }

}