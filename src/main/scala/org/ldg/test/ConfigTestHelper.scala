package org.ldg.test

import java.nio.file._

import com.typesafe.scalalogging.slf4j.LazyLogging


trait ConfigTestHelper extends FileTestHelper with LazyLogging {

  def cfgResources : Seq[String]

  def withConfig[A](env: String)(f: Seq[Path] => A) : A = {
    withTmpDir { tmpdir =>
      val configFilePaths = copyConfigsToDir(env, tmpdir.toString)
      f(configFilePaths)
    }
  }

  def copyConfigsToDir(env: String, destPath: String) : Seq[Path] = {
    cfgResources.map { resourceName =>
      Paths.get(copyResourceToFile(resourceName, destPath))
    }
  }
}
