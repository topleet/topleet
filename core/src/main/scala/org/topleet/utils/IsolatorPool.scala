package org.topleet.utils

import java.io.{File, ObjectInputStream, ObjectOutputStream}
import java.net._
import java.util.concurrent.TimeUnit

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future, _}
import scala.language.postfixOps


/**
 *  Comparable to [[org.topleet.utils.Isolator]] but reusing a pool of JVMs.
 */
object IsolatorPool {

  lazy val pool: Loading.Pool[Isolation] = {
    val isolations = (0 to 5).map(x => new Isolation(4000 + x))

    // TODO: delete all non necessary code.
    Runtime.getRuntime.addShutdownHook(new Thread() {
      for (i <- isolations)
        i.destroy()
    })
    Loading.pool(isolations)
  }

  def isolate[R](f: () => R, timeoutMs: Long = 1000 * 60 * 60): R =
    pool.use(x => x.submit(f, timeoutMs))

  // Start a sever socket for communication.

  class Isolation(port: Int) {

    def initialized(): Boolean = process.nonEmpty

    def alive(): Boolean = process.get.isAlive

    var process: Option[Process] = None
    var clientOIS: Option[ObjectInputStream] = None
    var clientOOS: Option[ObjectOutputStream] = None
    var clientSocket: Option[Socket] = None
    var serverSocket: Option[ServerSocket] = None

    def initialize(): Unit = {
      serverSocket = Some(new ServerSocket(port))

      // Initial Client JVM.
      val classpath: String = Isolator.getClass.getClassLoader.asInstanceOf[URLClassLoader].getURLs.map(_.getFile).reduce(_ + File.pathSeparator + _)
      val isolator = IsolatorPool.getClass.getName.substring(0, IsolatorPool.getClass.getName.length - 1)

      //"-Xmx15000m",
      //.inheritIO() "-Xss8m","-Xmx3500m" ,
      process = Some(new ProcessBuilder(System.getProperty("java.home") + "/bin/java", "-classpath", classpath, isolator, port.toString).start)
      // Accept client socket and get streams to client.
      clientSocket = Some(serverSocket.get.accept())
      clientOOS = Some(new ObjectOutputStream(clientSocket.get.getOutputStream))
      clientOIS = Some(new ObjectInputStream(clientSocket.get.getInputStream))
    }

    def submit[R](f: () => R, timeoutMs: Long): R = {
      // Destroy if not alive.
      if (initialized() && !alive()) destroy()

      // Initialize if the isolator is not running.
      if (!initialized()) initialize()
      // Send run function including the timeout which is applied in hte isolation.
      clientOOS.get.writeObject((f, timeoutMs))

      clientOIS.get.readObject() match {
        case x: Throwable => {
          destroy()
          throw x
        }
        case x: R => x
      }
    }

    def destroy(): Unit = {
      if (process.nonEmpty) {
        // Destroy process.
        process.get.destroy()

        process.get.waitFor(4, TimeUnit.SECONDS)
        if (process.get.isAlive) process.get.destroyForcibly()

        // Close things.
        clientOOS.get.close()
        clientOIS.get.close()
        clientSocket.get.close()
        serverSocket.get.close()

        process = None
        clientOOS = None
        clientOIS = None
        clientSocket = None
        serverSocket = None
      }
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

    // Run and submit result.
    try {
      while (true) {
        // Assign input
        val (f, timeoutMs) = clientOIS.readObject().asInstanceOf[((() => Any), Long)]
        val future = Future {
          f()
        }

        val result = Await.result(future, timeoutMs millis) match {
          case x: Throwable => throw x
          case x => x
        }

        clientOOS.writeObject(result)
      }
    } catch {
      case _: SocketException => // This is ok as indication closing isolation usage.
      case throwable: Throwable =>
        try {
          clientOOS.writeObject(throwable)
        } catch {
          case _: Throwable => // This is ok ??? as indication closing isolation usage.
        }
        // Close things.
        try {
          clientOOS.close()
        } catch {
          case _: Throwable => // This is ok ??? as indication closing isolation usage.
        }
        try {
          clientOIS.close()
        } catch {
          case _: Throwable => // This is ok ??? as indication closing isolation usage.
        }
        try {
          clientSocket.close()
        } catch {
          case _: Throwable => // This is ok ??? as indication closing isolation usage.
        }
    }
  }
}
