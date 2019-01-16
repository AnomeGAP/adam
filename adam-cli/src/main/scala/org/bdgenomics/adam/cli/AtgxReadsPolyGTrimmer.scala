package org.bdgenomics.adam.cli

import org.bdgenomics.formats.avro.AlignmentRecord

import scala.annotation.tailrec

// base on fastp
// https://github.com/OpenGene/fastp/blob/master/src/polyx.cpp
class AtgxReadsPolyGTrimmer {
  def trim(iter: Iterator[AlignmentRecord], compareReq: Int, maxMismatch: Int, allowOneMismatchForEach: Int): Iterator[AlignmentRecord] = {
    iter.map { record =>
      val seq = record.getSequence
      val len = seq.length
      val pos = findFirstGPos(seq, len - 1, len - 1,0, compareReq, maxMismatch, allowOneMismatchForEach)

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
  private def findFirstGPos(
    seq: String,
    firstGPos: Int,
    i: Int,
    mismatch: Int,
    compareReq: Int,
    maxMismatch: Int,
    allowOneMismatchForEach: Int): Int = {
    val allowedMismatch = (i + 1) / allowOneMismatchForEach
    if (mismatch > maxMismatch || (mismatch > allowedMismatch && i >= compareReq - 1) || i < 0) {
      firstGPos
    } else {
      if (seq(i) == 'G')
        findFirstGPos(seq, firstGPos - 1, i - 1, mismatch, compareReq, maxMismatch, allowOneMismatchForEach)
      else
        findFirstGPos(seq, firstGPos, i - 1, mismatch + 1, compareReq, maxMismatch, allowOneMismatchForEach)
    }
  }
}
