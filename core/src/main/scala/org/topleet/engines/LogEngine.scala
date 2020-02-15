package org.topleet.engines

import java.text.SimpleDateFormat
import java.util.Date

import org.topleet.{Engine, Leet}

import scala.reflect.{ClassTag => CT}


/**
 * An engine logging and printing the core operation calls to the console.
 * @param engine The underlying engine.
 */
case class LogEngine(engine: Engine) extends DelegateEngine(Seq(engine)) {

  var i = 0

  def log(i: Int, name: String, date: Date, stack: Seq[StackTraceElement]): Unit = {
    val message = "" +
      i +
      " " +
      name +
      " @ " +
      new SimpleDateFormat("dd/MM/yyyy HH:mm:ss").format(new Date()) +
      " ------------ " +
      "\n" +
      stack.map(_.toString).reduce(_ + "\n" + _)

    println(message)
  }

  override def beforeBinary[N2: CT, K2: CT, V2: CT, K3: CT, V3: CT](name: String, ls: Seq[(Leet[N2, Map[K2, V2]], Leet[N2, Map[K3, V3]])]): Seq[(Leet[N2, Map[K2, V2]], Leet[N2, Map[K3, V3]])] = {
    val stack = Thread.currentThread()
      .getStackTrace.toSeq.drop(3)


    log(i, name, new Date, stack)
    i = i + 1
    ls
  }

  override def beforeUnary[N2: CT, K2: CT, V2: CT](name: String, ls: Seq[Leet[N2, Map[K2, V2]]]): Seq[Leet[N2, Map[K2, V2]]] = {
    val stack = Thread.currentThread()
      .getStackTrace.toSeq.drop(3)

    log(i, name, new Date, stack)
    i = i + 1
    ls
  }
}
