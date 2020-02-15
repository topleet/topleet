package org.topleet.utils

import java.io.{File, ObjectInputStream, ObjectOutputStream}
import java.lang.management.{ManagementFactory, MemoryMXBean, MemoryType}
import java.net.{InetAddress, ServerSocket, Socket, URLClassLoader}
import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._

import scala.collection.mutable
import scala.concurrent.TimeoutException

/**
 * An single isolator stating a JVM with socket communication. A function is submitted and executed on the JVM and the result is
 * shipped back. The JVM can be profiled or timed out. The isolator uses the current classpath. The Isolator and the
 * corresponding [[org.topleet.utils.IsolatorPool]] are intended to execute analysis code that may cause memory issues and to profile
 * its applications.
 *
 */
object Isolator {

  def isolate[R](f: => R): R = {
    Isolator.isolate(() => f, 60 * 60 * 999)
  }

  def time(f: () => Unit, n: Int, timeout: Long): Seq[Long] = {
    isolate({ () =>
      val times = Seq(System.currentTimeMillis()) ++ (for (i <- 0 until n) yield {
        f()
        System.currentTimeMillis()
      })
      times.drop(1).zip(times).map { case (l, r) => l - r }
    }, timeout)
  }

  def tmprof(f: () => Unit, n: Int = 1, drop: Int = 0, timeout: Long = 60 * 60 * 24 * 32): (Long, Long) = {
    isolate({ () =>
      val bean = ManagementFactory.getMemoryPoolMXBeans.asScala.find(x => x.isUsageThresholdSupported && x.getType == MemoryType.HEAP).get
      val bufferTime = mutable.Buffer[Long]()
      val bufferMemory = mutable.Buffer[Long]()
      for (i <- 0 until n) {
        System.gc()
        bean.resetPeakUsage()
        val t1 = System.nanoTime()
        f()
        val t2 = System.nanoTime()
        val peak = bean.getPeakUsage

        if (i >= drop) {
          bufferTime.append(t2 - t1)
          bufferMemory.append(peak.getUsed)
        }
      }
      (bufferTime.sum / bufferTime.size.toLong, bufferMemory.sum / bufferMemory.size.toLong)
    }, timeout)
  }


  def mpeek(f: () => Unit): Long = {
    val mbean: MemoryMXBean = ManagementFactory.getMemoryMXBean
    val bean = ManagementFactory.getMemoryPoolMXBeans.asScala.find(x => x.isUsageThresholdSupported && x.getType == MemoryType.HEAP).get

    val p1 = bean.getPeakUsage
    bean.resetPeakUsage()
    f()
    val p2 = bean.getPeakUsage
    bean.resetPeakUsage()

    p2.getUsed - p1.getUsed
  }


  //getPeakUsage() method and reset by calling the resetPeakUsage()

  def isolate[R](f: () => R, timeout: Long = 60 * 60, port: Int = 4061): R = {
    // Start a sever socket for communication.
    val serverSocket = new ServerSocket(port)

    // Initial Client JVM.
    //val classpath: String = Thread.currentThread.getContextClassLoader.asInstanceOf[URLClassLoader].getURLs.map(_.getFile).reduce(_ + File.pathSeparator + _)
    //val classpath: String = Isolator.getClass.getClassLoader..get.getURLs.map(_.getFile).reduce(_ + File.pathSeparator + _)
    val classpath =  System.getProperty("java.class.path")
    val isolator = Isolator.getClass.getName.substring(0, IsolatorPool.getClass.getName.length - 1)
    val process = new ProcessBuilder(System.getProperty("java.home") + "/bin/java", "-Xss8m", "-Xmx15000m", "-classpath", classpath, isolator, port.toString).inheritIO().start
    // Accept client socket and get streams to client.
    val clientSocket = serverSocket.accept()
    val clientOOS = new ObjectOutputStream(clientSocket.getOutputStream)
    val clientOIS = new ObjectInputStream(clientSocket.getInputStream)

    // Send run function.
    clientOOS.writeObject(f)

    val exitCode = process.waitFor(timeout, TimeUnit.SECONDS)
    val r = if (exitCode) {
      // Consume result or exception.
      Some(clientOIS.readObject())
    }
    else {
      // Destroy process.
      process.destroy()
      process.waitFor(4, TimeUnit.SECONDS)
      if (process.isAlive) process.destroyForcibly()
      None
    }

    // Close things.
    clientOOS.close()
    clientOIS.close()
    clientSocket.close()
    serverSocket.close()
    r match {
      case Some(x: Throwable) => throw x
      case Some(x: R) => x
      case None => throw new TimeoutException
    }
  }

  // Isolation main.
  def main(args: Array[String]): Unit = {

    // Get target class
    val port = args(0).toInt
    // Register client socket.
    val clientSocket = new Socket(InetAddress.getByName("localhost"), port)

    // Get streams to server.
    val clientOOS = new ObjectOutputStream(clientSocket.getOutputStream)
    val clientOIS = new ObjectInputStream(clientSocket.getInputStream)

    // Assign input
    val f = clientOIS.readObject().asInstanceOf[() => Any]

    // Run and submit result.
    try {
      val result = f()
      clientOOS.writeObject(result)
    } catch {
      case throwable: Throwable => clientOOS.writeObject(throwable)
    }

    // Close things.
    clientOOS.close()
    clientOIS.close()
    clientSocket.close()
  }
}
