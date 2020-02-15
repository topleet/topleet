package org.topleet.git

import java.io.InputStream
import java.nio.charset.Charset

/**
 * A pointer to a resource in a git repository.
 * @param address The address of the repository.
 * @param objectId The objectId of the content.
 */
case class Resource(address: String, objectId: String) {
  def read(charset: String = "UTF-8"): String = Gitobject(address).read(objectId, Charset.forName(charset))

  def inputStream(): InputStream = Gitobject(address).openStream(objectId)
}
