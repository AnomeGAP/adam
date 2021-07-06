package org.bdgenomics.adam.cli.piper

import scala.util.matching.Regex

object Util {

  val pattern: Regex = "(abfs[s]?)://([0-9a-zA-Z-_]+)@([0-9a-zA-Z-_]+).dfs.core.windows.net(/.*)".r

  def gen2ToHdfs(path: String): String = {
    pattern
      .findFirstMatchIn(path)
      .map(_.group(4)) match {
        case Some(i) => i
        case None    => throw new RuntimeException(s"$path is not Gen2 path")
      }
  }
}
