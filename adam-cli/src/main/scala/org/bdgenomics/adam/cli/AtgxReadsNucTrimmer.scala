package org.bdgenomics.adam.cli

import org.bdgenomics.formats.avro.AlignmentRecord

import scala.annotation.tailrec

class AtgxReadsNucTrimmer {
  def trimHead(iter: Iterator[AlignmentRecord], tenX: Boolean, minLen: Int): Iterator[AlignmentRecord] = {
    iter.map { record =>
      val name = record.getReadName
      val (_, iw) = AtgxReadsInfoParser.parseFromName(name)
      // use readId to identify read1 read2
      if ((iw.getID & 0x1) == 0) {
        if (tenX) record else trimHeadN(record)
      } else {
        trimHeadN(record)
      }
    }.filter { i =>
      val seq = i.getSequence
      seq.nonEmpty && seq.length >= minLen
    }
  }

  def trimTail(iter: Iterator[AlignmentRecord], minLen: Int): Iterator[AlignmentRecord] = {
    iter.map(trimTailN)
      .filter { i =>
        val seq = i.getSequence
        seq.nonEmpty && seq.length >= minLen
      }
  }

  def trimBoth(iter: Iterator[AlignmentRecord], tenX: Boolean, minLen: Int): Iterator[AlignmentRecord] = {
    iter.map { record =>
      val name = record.getReadName
      val (_, iw) = AtgxReadsInfoParser.parseFromName(name)
      // use readId to identify read1 read2
      if ((iw.getID & 0x1) == 0) {
        if (tenX) trimTailN(record) else (trimHeadN _ andThen trimTailN)(record)
      } else {
        (trimHeadN _ andThen trimTailN)(record)
      }
    }.filter { i =>
      val seq = i.getSequence
      seq.nonEmpty && seq.length >= minLen
    }
  }

  private def trimHeadN(record: AlignmentRecord): AlignmentRecord = {
    val seq = record.getSequence
    val trimmedSeq = trimH(seq)
    record.setSequence(trimmedSeq)

    val len = seq.length - trimmedSeq.length
    val newQuality = record.getQual.substring(len)
    record.setQual(newQuality)

    record
  }

  private def trimTailN(record: AlignmentRecord): AlignmentRecord = {
    val seq = record.getSequence
    val trimmedSeq = trimT(seq)
    record.setSequence(trimmedSeq)

    val newQuality = record.getQual.substring(0, trimmedSeq.length)
    record.setQual(newQuality)

    record
  }

  private def trimH(seq: String): String = {
    @tailrec
    def aux(seq: String, idx: Int): String = {
      if (seq.head == 'N')
        aux(seq.tail, idx + 1)
      else
        seq
    }

    aux(seq, 0)
  }

  private def trimT(seq: String): String = {
    @tailrec
    def aux(seq: String, idx: Int): String = {
      if (seq(idx) == 'N')
        aux(new String(seq.substring(0, idx)), idx - 1)
      else
        seq
    }

    aux(seq, seq.length - 1)
  }
}
