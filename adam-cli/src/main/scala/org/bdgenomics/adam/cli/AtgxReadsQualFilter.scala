package org.bdgenomics.adam.cli

import org.bdgenomics.formats.avro.AlignmentRecord

import scala.annotation.switch

class AtgxReadsQualFilter extends java.io.Serializable {
  def filterReads(iter: Iterator[AlignmentRecord], minQual: Int = 63, maxCount: Int = 10, invFlag: Boolean = false): Iterator[AlignmentRecord] = {
    iter
      .flatMap(
        x => {
          val failCount = x.getQual
            .foldLeft(0) {
              (sum, c) => {
                // currently support Illumina 1.8+ Phred+33 quality scheme, with value range of 33 - 73
                assert(c.toInt <= 75 && c.toInt >= 33)
                if (c.toInt < minQual) sum + 1
                else sum
              }
            }
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
