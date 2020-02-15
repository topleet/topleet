package org.topleet.git

import java.io.File
import java.nio.charset.Charset
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.commons.io.IOUtils
import org.eclipse.jgit.api.{BlameCommand, Git}
import org.eclipse.jgit.diff.DiffEntry.ChangeType
import org.eclipse.jgit.lib._
import org.eclipse.jgit.revwalk.RevWalk
import org.eclipse.jgit.storage.file.FileRepositoryBuilder
import org.eclipse.jgit.treewalk.{CanonicalTreeParser, TreeWalk}
import org.topleet.Topleets
import org.topleet.utils.Loading

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * A wrapper around the JGit API for a git repository.
 */
object Gitobject {

  private def fsload(address: String): Repository = Loading.fileSystemLoadingCache(address) {
    case (s: String, f: File) =>
      val git = Git.cloneRepository.setBare(true).setGitDir(f).setURI("https://github.com/" + s + ".git").call
      git.getRepository.close()
      git.close()
  } { case (_: String, f: File) =>
    val repositoryBuilder = new FileRepositoryBuilder();
    repositoryBuilder.setMustExist(true)
    repositoryBuilder.setGitDir(f)
    repositoryBuilder.build()
  }

  val buffer = Loading.threadLocalLoadingCache(1)((s: String) => fsload(s))(t => t.close())

}

/**
 * A wrapper around the JGit API for a git repository.
 */
case class Gitobject(address: String) {

  private def canonicalTreeParser(id: ObjectId): CanonicalTreeParser = {
    val canonicalTreeParser = new CanonicalTreeParser()
    canonicalTreeParser.reset(repository().newObjectReader(), id)
    canonicalTreeParser
  }

  def repository(): Repository = Gitobject.buffer.get().get(address)

  def parents(commit: String) =
    new RevWalk(repository()).parseCommit(ObjectId.fromString(commit)).getParents.map(x => ObjectId.toString(x))

  def parentsCount(commit: String) =
    new RevWalk(repository()).parseCommit(ObjectId.fromString(commit)).getParentCount

  def time(commit: String): Int = new RevWalk(repository()).parseCommit(ObjectId.fromString(commit)).getCommitTime

  def comment(commit: String): String = new RevWalk(repository()).parseCommit(ObjectId.fromString(commit)).getFullMessage

  def authorMail(commit: String): String = new RevWalk(repository()).parseCommit(ObjectId.fromString(commit)).getAuthorIdent.getEmailAddress

  def authorName(commit: String): String = new RevWalk(repository()).parseCommit(ObjectId.fromString(commit)).getAuthorIdent.getName

  def read(objectId: String, charset: Charset = Charset.forName("UTF-8")): String = IOUtils.toString(repository().open(ObjectId.fromString(objectId)).openStream(), charset)

  def lines(objectId: String, charset: Charset = Charset.forName("UTF-8")): Int = IOUtils.readLines(repository().open(ObjectId.fromString(objectId)).openStream(), charset).size

  def openStream(objectId: String): ObjectStream = repository().open(ObjectId.fromString(objectId)).openStream()

  def commits(): Seq[String] = {
    val headId = repository().resolve(Constants.HEAD)
    if (headId == null) return Seq()

    val head = new RevWalk(repository()).parseCommit(headId)

    val rw = new RevWalk(repository())
    rw.markStart(head)
    rw.iterator().asScala.toSeq.map(x => ObjectId.toString(x.getId))
  }

  /**
   * @return Return path objectId tuples.
   */
  def tree(commit: String, suffix: String = ""): Set[(String, String)] = {
    val suffixBytes = suffix.getBytes
    val treeWalk = new TreeWalk(repository())
    treeWalk.addTree(new RevWalk(repository()).parseCommit(ObjectId.fromString(commit)).getTree)
    treeWalk.setRecursive(true)

    val result = mutable.HashSet[(String, String)]()
    while (treeWalk.next()) {
      if (suffix == "" || treeWalk.isPathSuffix(suffixBytes, suffixBytes.length))
        result.add((treeWalk.getPathString, ObjectId.toString(treeWalk.getObjectId(0))))
    }

    result.toSet
  }

  def tree2(commit: String, suffix: String = ""): Map[String, Resource] =
    tree(commit, suffix).toMap.map { case (path, resid) => (path, Resource(address, resid)) }

  def blame(path: String, objectId: String): Map[(String, String), Int] = {
    val blamer = new BlameCommand(repository)
    blamer.setFilePath(path)
    blamer.setFollowFileRenames(true)

    val blame = blamer.call

    if (blame == null) return Map()

    val lineAuthors = for (i <- 0 until blame.getResultContents.size())
      yield (blame.getSourceCommit(i).getAuthorIdent.getName, blame.getSourceCommit(i).getAuthorIdent.getEmailAddress)

    lineAuthors.map(x => (x, 1)).groupBy(_._1).map { case (author, group) => (author, group.map(_._2).sum) }
  }

  val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")

  def defaultAttributes(sha: String): Map[String, String] = Map(
    "date" -> dateFormat.format(new Date(time(sha) * 1000L)),
    "fix" -> Topleets.tokenize(Topleets.toLower(Topleets.keepLetters(comment(sha))))
      .exists { x => (x.contains("fix") && !x.contains("prefix") && !x.contains("postfix")) }
      .toString,
    "issue" -> Topleets.tokenize(Topleets.toLower(Topleets.keepLetters(comment(sha))))
      .exists { x => x == "issue" || x == "issues" }
      .toString,
    "address" -> address,
    "sha" -> sha,
    "parents" -> parents(sha).length.toString,
    "name" -> authorName(sha),
    "mail" -> authorMail(sha))


  def diff(last: Option[String], current: Option[String]): Seq[(String, Option[String], Option[String])] = {
    (last, current) match {
      case (Some(l), Some(c)) =>
        new Git(repository()).diff().setShowNameAndStatusOnly(true)
          .setOldTree(canonicalTreeParser(new RevWalk(repository()).parseCommit(ObjectId.fromString(l)).getTree.getId))
          .setNewTree(canonicalTreeParser(new RevWalk(repository()).parseCommit(ObjectId.fromString(c)).getTree.getId))
          .call().asScala.map {
          diff =>
            (diff.getChangeType match {
              case ChangeType.DELETE => diff.getOldPath
              case _ => diff.getNewPath
            },
              diff.getChangeType match {
                case ChangeType.ADD => None
                case _ => Some(ObjectId.toString(diff.getOldId.toObjectId))
              },
              diff.getChangeType match {
                case ChangeType.DELETE => None
                case _ => Some(ObjectId.toString(diff.getNewId.toObjectId))
              })
        }
      case (None, Some(c)) => tree(c).map { case (path, objectId) => (path, None, Some(objectId)) }.toSeq
      case (Some(l), None) => tree(l).map { case (path, objectId) => (path, Some(objectId), None) }.toSeq
      case (None, None) => Seq()
    }
  }

  def diff2(last: Option[String], current: Option[String]): (Map[String, Resource], Map[String, Resource]) = {
    val removes = diff(last, current).collect {
      case (path, Some(x), _) => path -> Resource(address, x)
    }
    val adds = diff(last, current).collect {
      case (path, _, Some(x)) => path -> Resource(address, x)
    }
    (removes.toMap, adds.toMap)
  }

}
