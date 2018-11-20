package org.bdgenomics.adam.cli

import org.bdgenomics.formats.avro.AlignmentRecord

class AtgxReadsRandNucAssigner {
  /**
   * do random nucleotide assignment to N base of input sequence
   */
  def assign(iter: Iterator[AlignmentRecord]): Iterator[AlignmentRecord] = {
    val alphabet = Array[Char]('A', 'C', 'G', 'T')
    val randGen = scala.util.Random

    iter.toList
      .map { record =>
        val replacedSeq = record.getSequence.map { c =>
          if (c == 'N')
            alphabet(randGen.nextInt(4))
          else
            c
        }
        record.setSequence(replacedSeq)
        record
      }
      .toIterator
  }
}