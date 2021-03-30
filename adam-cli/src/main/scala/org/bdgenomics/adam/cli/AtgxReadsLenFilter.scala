package org.bdgenomics.adam.cli

import org.bdgenomics.formats.avro.Alignment

class AtgxReadsLenFilter {
  def filterLen(iter: Iterator[Alignment], minLen: Int): Iterator[Alignment] = {
    iter.filter { record =>
      record.getSequence.length >= minLen
    }
  }
}
