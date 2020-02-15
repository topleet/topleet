package org.topleet.git

/**
 * The identification of a Git commit.
 * @param address The address of the repository.
 * @param sha The sha of the commit.
 */
case class SHA(address: String, sha: Option[String]) {

  override def toString: String = sha.getOrElse("root")

  def link() = s"https://github.com/$address/commit/${sha.getOrElse("")}"

}
