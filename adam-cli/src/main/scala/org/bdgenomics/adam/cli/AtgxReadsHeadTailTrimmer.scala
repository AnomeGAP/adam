package org.bdgenomics.adam.cli

import org.bdgenomics.formats.avro.AlignmentRecord

class AtgxReadsHeadTailTrimmer extends Serializable {
  def trim(iter: Iterator[AlignmentRecord]): Iterator[AlignmentRecord] = {
    iter.map { record =>
      val len = record.getSequence.length
      val newSeq = new String(record.getSequence.substring(1, len - 2))
      record.setSequence(newSeq)
      val newQuality = new String(record.getQuality.substring(1, len - 2))
      record.setQuality(newQuality)

      record
    }
  }
}
