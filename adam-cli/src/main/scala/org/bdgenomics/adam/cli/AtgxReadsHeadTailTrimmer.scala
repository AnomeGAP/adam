package org.bdgenomics.adam.cli

import org.bdgenomics.formats.avro.AlignmentRecord

class AtgxReadsHeadTailTrimmer {
  def trimHead(iter: Iterator[AlignmentRecord]): Iterator[AlignmentRecord] = {
    iter.map { record =>
      val newSeq = new String(record.getSequence.substring(1))
      record.setSequence(newSeq)
      val newQuality = new String(record.getQual.substring(1))
      record.setQual(newQuality)

      record
    }
  }

  def trimTail(iter: Iterator[AlignmentRecord]): Iterator[AlignmentRecord] = {
    iter.map { record =>
      val len = record.getSequence.length
      val newSeq = new String(record.getSequence.substring(0, len - 2))
      record.setSequence(newSeq)
      val newQuality = new String(record.getQual.substring(0, len - 2))
      record.setQual(newQuality)

      record
    }
  }
}
