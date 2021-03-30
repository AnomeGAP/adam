package org.bdgenomics.adam.cli

import org.bdgenomics.formats.avro.Alignment

class AtgxReadsMultipleNFilter {
  def filterN(iter: Iterator[Alignment], maxN: Int, invFlag: Boolean): Iterator[Alignment] = {
    iter.filter { record =>
      // TODO: use tailrec
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
