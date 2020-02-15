package org.topleet.engines

import org.topleet.{Engine, Leet}

import scala.reflect.{ClassTag => CT}

/**
 * An engine checking the correspondence of the results in different underlying engines.
 * @param engines The engines where the results have to correspond.
 * @param onlyIndexCalls A flag whether the correspondence should be checked after each call to a core operation or just
 *                       for calls to index().
 */
class CheckEngine(engines: Seq[Engine], onlyIndexCalls: Boolean = false) extends DelegateEngine(engines) {

  import org.topleet.libs.Natives._

  override def checkIndex[N1: CT, K1: CT, V1: CT](leet: DELLeet[N1, K1, V1]): Iterator[(N1, Map[K1, V1])] = {
    check(leet.nested)
    super.checkIndex(leet)
  }

  def check[N2: CT, K2: CT, V2: CT](ls: Seq[Leet[N2, Map[K2, V2]]]): Unit = {
    println("check")
    val indexes = ls.map(x => x.index().toMap)

    if (indexes.toSet.size != 1) {
      for (n <- ls.head.nodes()) {
        val current = indexes.map(x => x.get(n))
        if (current.toSet.size != 1) {
          println("---------------------------------------------------------------")
          println("Node " + n)
          for ((x, i) <- current.zipWithIndex) {
            println(ls(i).getClass.getName)
            println("Index : " + x)
          }
        }
      }
      assert(false)
    }
  }

  override def after[N2: CT, K2: CT, V2: CT](name: String, ls: Seq[Leet[N2, Map[K2, V2]]]): Seq[Leet[N2, Map[K2, V2]]] = {
    if (!onlyIndexCalls)
      check(ls)

    ls
  }
}