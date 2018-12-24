package org.bdgenomics.adam.cli

import org.bdgenomics.formats.avro.AlignmentRecord

class AtgxReadsLenFilter {
  def filterLen(iter: Iterator[AlignmentRecord], minLen: Int): Iterator[AlignmentRecord] = {
    iter.filter { record =>
      record.getSequence.length >= minLen
    }
  }
}
