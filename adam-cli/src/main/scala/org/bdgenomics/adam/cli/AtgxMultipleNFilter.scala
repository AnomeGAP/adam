package org.bdgenomics.adam.cli

import org.bdgenomics.formats.avro.AlignmentRecord

class AtgxMultipleNFilter {
  def filterN(iter: Iterator[AlignmentRecord], maxN: Int): Iterator[AlignmentRecord] = {
    iter.toList
      .filter { record =>
        val nCount = record.getSequence.filter(_ == 'N').length
        if (nCount > maxN)
          false
        else
          true
      }
      .toIterator
  }
}
