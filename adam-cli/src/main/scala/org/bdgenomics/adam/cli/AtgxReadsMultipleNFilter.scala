package org.bdgenomics.adam.cli

import org.bdgenomics.formats.avro.AlignmentRecord

class AtgxReadsMultipleNFilter {
  def filterN(iter: Iterator[AlignmentRecord], maxN: Int, invFlag: Boolean): Iterator[AlignmentRecord] = {
    iter.filter { record =>
      val nCount = record.getSequence.filter(_ == 'N').length

      if (!invFlag)
        if (nCount > maxN)
          false
        else
          true
      else if (nCount > maxN)
        true
      else
        false
    }
  }
}
