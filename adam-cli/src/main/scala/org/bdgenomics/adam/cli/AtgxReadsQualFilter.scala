package org.bdgenomics.adam.cli

import org.bdgenomics.formats.avro.AlignmentRecord

import util.control.Breaks._
import scala.annotation.{switch, tailrec}

class AtgxReadsQualFilter extends java.io.Serializable {

  @tailrec
  private def scan(qs: String, minQ: Int, maxCount: Int, accum: Int): Int = {
    assert(qs.head.toInt <= 75 && qs.head.toInt >= 33)

    if (accum < maxCount){
      if (qs.head.toInt < minQ)
        scan(new String(qs.substring(1)), minQ, maxCount, accum + 1)
      else
        scan(new String(qs.substring(1)), minQ, maxCount, accum)
    }
    else
      accum
  }

  def filterReads(iter: Iterator[AlignmentRecord], minQual: Int = 63, maxCount: Int = 10, invFlag: Boolean = false): Iterator[AlignmentRecord] = {
    iter
      .flatMap(
        x => {
          val failCount = scan(x.getQual.reverse, minQual, maxCount, 0)
          if (! invFlag) {
            if (failCount < maxCount)
              Some(x)
            else
              None
          }
          else {
            if (failCount < maxCount)
              None
            else
              Some(x)
          }
        }
      )
  }
}
