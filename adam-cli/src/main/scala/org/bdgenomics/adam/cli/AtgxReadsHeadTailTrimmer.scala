package org.bdgenomics.adam.cli

import org.bdgenomics.formats.avro.Alignment

class AtgxReadsHeadTailTrimmer extends Serializable {
  def trim(iter: Iterator[Alignment]): Iterator[Alignment] = {
    iter.map { record =>
      val len = record.getSequence.length
      val newSeq = new String(record.getSequence.substring(1, len - 2))
      record.setSequence(newSeq)
      val newQuality = new String(record.getQualityScores.substring(1, len - 2))
      record.setQualityScores(newQuality)

      record
    }
  }
}
