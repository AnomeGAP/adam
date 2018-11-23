package org.bdgenomics.adam.cli

import org.bdgenomics.formats.avro.AlignmentRecord

class AtgxReadsNucTrimmer {
  def trimHead(iter: Iterator[AlignmentRecord], tenX: Boolean): Iterator[AlignmentRecord] = {
    val array = iter.toArray
    val length = array.length
    val read1 = array.slice(0, length / 2)
    val read2 = array.slice(length / 2, length)

    val trimmedR1 = if (tenX) {
      read1
    } else {
      read1.map(trimHeadN)
    }

    (trimmedR1 ++ read2.map(trimHeadN))
      .toIterator
  }

  def trimTail(iter: Iterator[AlignmentRecord]): Iterator[AlignmentRecord] = {
    iter.toList.map(trimTailN).toIterator
  }

  def trimBoth(iter: Iterator[AlignmentRecord], tenX: Boolean): Iterator[AlignmentRecord] = {
    val array = iter.toArray
    val length = array.length
    val read1 = array.slice(0, length / 2)
    val read2 = array.slice(length / 2, length)

    val trimmedRead1 = if (tenX) {
      read1.map(trimTailN)
    } else {
      read1.map(trimHeadN _ andThen trimTailN)
    }
    val trimmedRead2 = read2.map(trimHeadN _ andThen trimTailN)

    (trimmedRead1 ++ trimmedRead2).toIterator
  }

  private def trimHeadN(record: AlignmentRecord): AlignmentRecord = {
    val seq = record.getSequence
    val trimmedSeq = trimH(seq)
    record.setSequence(trimmedSeq)
    record
  }

  private def trimTailN(record: AlignmentRecord): AlignmentRecord = {
    val seq = record.getSequence
    val trimmedSeq = trimT(seq)
    record.setSequence(trimmedSeq)
    record
  }

  private def trimH(seq: String): String = seq.replaceAll("^N*", "")
  private def trimT(seq: String): String = seq.replaceAll("N*$", "")
}
