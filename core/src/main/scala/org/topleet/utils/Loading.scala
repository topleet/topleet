package org.topleet.utils

import java.io.{File, RandomAccessFile}
import java.net.URLEncoder
import java.nio.channels.OverlappingFileLockException
import java.util.concurrent.{ArrayBlockingQueue, BlockingQueue}

import com.google.common.cache._
import org.apache.commons.io.FileUtils

/**
 * Utils for loading and reusing.
 */
object Loading {

  def threadLocalLoadingCache[S <: Object, T <: Object](maximumSize: Int)(onLoad: S => T)(onUnload: T => Unit): ThreadLocal[LoadingCache[S, T]] = new ThreadLocal[LoadingCache[S, T]]() {
    override def initialValue(): LoadingCache[S, T] = {
      CacheBuilder.newBuilder()
        .concurrencyLevel(1)
        .maximumSize(maximumSize)
        .removalListener(new RemovalListener[S, T] {
          override def onRemoval(notification: RemovalNotification[S, T]): Unit = {
            onUnload(notification.getValue)
          }
        })
        .build(
          new CacheLoader[S, T] {
            override def load(key: S): T = onLoad(key)
          }
        )
    }
  }

  class Pool[T](ts: Seq[T]) {
    val bc: BlockingQueue[T] = new ArrayBlockingQueue[T](ts.size)
    ts.foreach(bc.offer)

    def use[R](f: T => R): R = {
      val t = bc.take()
      val r = try {
        f(t)
      }
      finally {
        bc.offer(t)
      }
      r
    }
  }

  def pool[T](ts: Seq[T]) = new Pool[T](ts)

  def fileSystemLoadingCache[S, T](key: S)(onLoad: (S, File) => Unit)(accessor: (S, File) => T): T = {

    def keyFolder(key: S) = new File(Configuration.get("topleet_temp") + "/resources/" + "/" + URLEncoder.encode(key.toString.replace("*", "x"), "UTF-8"))

    def lockFile(key: S) = new File(keyFolder(key), "lock")

    def readyFile(key: S) = new File(keyFolder(key), "ready")

    def contentFolder(key: S) = new File(keyFolder(key), "content")

    def waitForLoad(key: S): Unit = {
      while (!readyFile(key).exists()) {
        Thread.sleep(1000)
      }
    }

    if (!readyFile(key).exists())
      try {
        keyFolder(key).mkdirs()

        if (!lockFile(key).exists()) lockFile(key).createNewFile()

        val file = new RandomAccessFile(lockFile(key), "rw");
        try {
          val lock = file.getChannel.tryLock()
          if (lock != null) {
            if (!readyFile(key).exists()) {
              FileUtils.deleteDirectory(contentFolder(key))

              // Load resource.
              onLoad(key, contentFolder(key))

              readyFile(key).createNewFile()
              lock.release()
            }
          }
        } catch {
          case _: OverlappingFileLockException =>
        }
        finally {
          file.close()
        }
      }

    waitForLoad(key)

    accessor(key, contentFolder(key))
  }
}
