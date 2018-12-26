package org.bdgenomics.adam.cli

import org.bdgenomics.formats.avro.AlignmentRecord

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

  // TODO: not use replaceAll
  private def trimH(seq: String): String = seq.replaceAll("^N*", "")
  private def trimT(seq: String): String = seq.replaceAll("N*$", "")
}
