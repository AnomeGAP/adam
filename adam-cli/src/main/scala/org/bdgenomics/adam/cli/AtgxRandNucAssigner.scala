package org.bdgenomics.adam.cli

import org.apache.spark.TaskContext
import org.bdgenomics.formats.avro.AlignmentRecord

import scala.annotation.switch
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class AtgxRandNucAssigner {
  /**
   * do random nucleotide assignment to N base of input sequence
   */
  def assign(iter: Iterator[AlignmentRecord], maxN: Int): Iterator[AlignmentRecord] = {
    val alphabet = Array[Char]('A', 'C', 'G', 'T')
    val randGen = scala.util.Random
    val res = new ArrayBuffer[AlignmentRecord]()

    while (iter.hasNext) {
      val entry = iter.next()
      val seq = entry.getSequence

      var countN = 0
      val strBuf = new mutable.StringBuilder()
      for (c <- seq) {
        (c: @switch) match {
          case 'N' =>
            countN += 1
            strBuf += alphabet(randGen.nextInt(4))
          case _ =>
            strBuf += c
        }
      }
      if (countN <= maxN){
        entry.setSequence(strBuf.toString)
        res.append(entry)
      }
    }
    res.iterator
  }
}