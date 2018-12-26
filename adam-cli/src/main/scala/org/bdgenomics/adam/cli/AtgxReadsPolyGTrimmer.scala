package org.bdgenomics.adam.cli

import org.bdgenomics.formats.avro.AlignmentRecord

import scala.annotation.tailrec

class AtgxReadsPolyGTrimmer {
  def trim(iter: Iterator[AlignmentRecord], compareReq: Int = 10): Iterator[AlignmentRecord] = {
    iter.map { record =>
      val seq = record.getSequence
      val pos = findFirstGPos(seq, seq.length - 1, 0, seq.length, 0, compareReq)

      if (pos >= 0) {
        val trimmedSeq = new String(seq.substring(0, pos))
        val trimmedQuality = new String(record.getQual.substring(0, pos))
        record.setSequence(trimmedSeq)
        record.setQual(trimmedQuality)
      }

      record
    }
  }

  @tailrec
  private def findFirstGPos(seq: String, firstGPos: Int, i: Int, len: Int, mismatch: Int, compareReq: Int): Int = {
    val maxMismatch = 5
    val allowOneMismatchForEach = 8

    val allowedMismatch = (i + 1) / allowOneMismatchForEach
    if (mismatch > maxMismatch || (mismatch > allowedMismatch && i >= compareReq - 1) || i >= len) {
      firstGPos
    } else {
      if (seq(i) == 'G')
        findFirstGPos(seq, firstGPos - 1, i + 1, len, mismatch, compareReq)
      else
        findFirstGPos(seq, firstGPos, i + 1, len, mismatch + 1, compareReq)
    }
  }
}
