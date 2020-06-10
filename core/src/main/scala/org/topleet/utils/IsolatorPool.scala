package org.topleet.utils

import java.io.{File, ObjectInputStream, ObjectOutputStream}
import java.net._
import java.util.concurrent.{Executors, TimeUnit}

import org.topleet.utils.Loading
//import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future, _}


/**
 * Comparable to [[org.topleet.utils.Isolator]] but reusing a pool of JVMs.
 */
object IsolatorPool {

  val n = 4

  /**
   * This needs to be a separate resource as it might block with the Loading pool.
   * TODO: This needs some better idea of threading.
   */
  val es = Executors.newCachedThreadPool()
  implicit val ec = ExecutionContext.fromExecutor(es)

  lazy val isolations: Seq[IsolationInner] = (0 until n).map(x => new IsolationInner(4000 + x))

  lazy val pool: Loading.Pool[IsolationInner] = Loading.pool(isolations)

  def isolate[R](f: () => R, timeoutMs: Long = 1000 * 60 * 60): R =
    pool.use(x => x.submit(f, timeoutMs))

  // Start a sever socket for communication.

  class IsolationInner(port: Int) {

    def initialized(): Boolean = process.nonEmpty

    var process: Option[Process] = None
    var clientOIS: Option[ObjectInputStream] = None
    var clientOOS: Option[ObjectOutputStream] = None
    var clientSocket: Option[Socket] = None
    var serverSocket: Option[ServerSocket] = None

    def initialize(): Unit = {
      //println("initialize")
      serverSocket = Some(new ServerSocket(port))

      // Initial Client JVM.
      val classpath: String = IsolatorPool.getClass.getClassLoader.asInstanceOf[URLClassLoader].getURLs.map(_.getFile).reduce(_ + File.pathSeparator + _)
      val isolator = IsolatorPool.getClass.getName.substring(0, IsolatorPool.getClass.getName.length - 1)

      //"-Xmx15000m",
      //.inheritIO() "-Xss8m","-Xmx3500m" ,
      process = Some(new ProcessBuilder(System.getProperty("java.home") + "/bin/java", "-classpath", classpath, isolator, port.toString).inheritIO().start)
      // Accept client socket and get streams to client.
      clientSocket = Some(serverSocket.get.accept())
      clientOOS = Some(new ObjectOutputStream(clientSocket.get.getOutputStream))
      clientOIS = Some(new ObjectInputStream(clientSocket.get.getInputStream))
      //println("done")
    }

    def submit[R](f: () => R, timeoutMs: Long): R = {
      // Initialize if the isolator is not running.
      if (!initialized()) initialize()
      // Send run function including the timeout which is applied in the isolation.
      //println("submit")
      val future = Future {
        //println("real submit")
        clientOOS.get.writeObject(f)
        clientOIS.get.readObject()
      }

      val resultOrException = try {
        Await.result(future, timeoutMs millis)
      } catch {
        case e: Throwable => {
          //println(e)
          e
        }
      }

      //println("submitted")
      resultOrException match {
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
        process.get.destroyForcibly()
        process.get.waitFor()

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

    //println("starting")
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
        //println("waiting")
        val f = clientOIS.readObject().asInstanceOf[() => Any]
        //println("working")
        val result = f()
        clientOOS.writeObject(result)
      }
    } catch {

      case e: Throwable => {
        //println(e)
        clientOOS.writeObject(e)
      }
    }
  }
}
