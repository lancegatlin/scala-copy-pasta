package org.ldg.test

import java.nio.file.{Files, Path}

import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.commons.io.FileUtils
import org.scalatest.Matchers

import scala.io.Source
import org.ldg._

trait FileTestHelper extends Matchers with LazyLogging {
  def testName: String = this.getClass.getSimpleName

  def withTmpDir[A](f: Path => A) : A = {
    val tmpdir = Files.createTempDirectory(s"$testName-")
    logger.info(s"Created temp directory: $tmpdir")
    try {
      f(tmpdir)
    } finally {
      logger.info(s"Removing temp directory: $tmpdir")
      rmDir(tmpdir.toString)
    }
  }

  def ls(path: String) : List[java.io.File] = {
    {
      new java.io.File(path)
    }
      .listFiles
      .toList
  }

  def getResourceAsString(name: String) : String = {
    require(name.startsWith("/"), s"Resource name must start with slash(/): $name")
    val resIn = Option(getClass.getResourceAsStream(name)).getOrDie(s"Failed to find resource: $name")
    Source.fromInputStream(resIn).mkString
  }

  def getResourceAsOneLineString(name: String) : String =
    getResourceAsString(name).replaceAll("\\s+", " ")

  /**
    * Compare to strings line by line
    * @param lhs expect
    * @param rhs
    * @return
    */
  def compareLines(lhs: String, rhs: String) : Option[(Int,String,String)] = {
    val lhsLines = lhs.lines.toIndexedSeq
    val rhsLines = rhs.lines.toIndexedSeq
    lhsLines.iterator.zip(rhsLines.iterator).zipWithIndex.collectFirst {
      case ((lhsLine,rhsLine),idx) if lhsLine != rhsLine => (idx,lhsLine,rhsLine)
    }
    if(lhsLines.size < rhsLines.size) {
      val idx = lhsLines.size
      Some((idx,"<missing>",rhsLines(idx)))
    } else if(lhsLines.size > rhsLines.size) {
      val idx = rhsLines.size
      Some((idx,lhsLines(idx),"<missing>"))
    } else {
      None
    }
  }

  def ensureSameLines(expected: String, found: String) : Unit = {
    compareLines(expected, found).foreach { case (idx,expectedLine,foundLine) =>
      val prefix = s"[line ${idx+1}]"
      // note: this will always fail at this point
      (prefix,foundLine) shouldBe (prefix,expectedLine)
    }
  }

  def rmDir(path: String) : Unit = {
    val dir = new java.io.File(path)
    if(FileUtils.deleteQuietly(dir) == false) {
      logger.warn(s"Could not delete path=$path, will delete on exit")
      FileUtils.forceDeleteOnExit(dir)
    }
  }

  def copyResourceToFile(name: String, destPath: String) : String = {
    require(name.startsWith("/"), s"Resource name must start with slash(/): $name")
    val resIn = Option(getClass.getResourceAsStream(name)).getOrDie(s"Failed to find resource: $name")
    val fullDestPath = destPath + name
    FileUtils.copyInputStreamToFile(resIn, new java.io.File(fullDestPath))
    fullDestPath
  }
}
