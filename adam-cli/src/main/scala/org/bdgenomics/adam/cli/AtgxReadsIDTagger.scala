package org.bdgenomics.adam.cli

import org.apache.spark.TaskContext
import org.bdgenomics.formats.avro.AlignmentRecord

import scala.collection.mutable.ArrayBuffer

object AtgxReadsIDTagger {}

class AtgxReadsIDTagger {
  /**
    * we will use 40 bit of a long to do reads id encoding (~1 trillions) in StringGraph
    * we reserve 256M serial numbers for reads ID for each partition =>
    * the overall partition number upper bound is 4096
    */
  def tag(iter: Iterator[AlignmentRecord], partitionSerialOffset: Int = 268435456): Iterator[AlignmentRecord] = {
    val content = iter.toArray
    val pairBound = content.length / 2
    val itr = content.iterator
    val serialOffset = TaskContext.getPartitionId().toLong * partitionSerialOffset
    var counter = 0

    /**
      * INPUT:
      * the pair-one reads and pair-two reads are aggregated, following the same order, in the first half
      * and the second half of the input chunk files.
      *
      * OUTPUT:
      * read1 is renamed with a serial number N, and its mate read2 is renamed with N+1, e.g.
      *
      * input file                               output file
      * ---------------------                    ---------------------
      * @ read1 /1                               @ read1 /1 000000
      * @ read2 /1                               @ read2 /1 000002
      * ...                                      ...
      * @ readN /1                               @ readN /1 00000{M}
      * ---------------------       ====>        ---------------------
      * @ read1 /2                               @ read1 /2 000001
      * @ read2 /2                               @ read2 /2 000003
      * ...                                      ...
      * @ readN /2                               @ readN /2 00000${M+1}
      * ---------------------                    ---------------------
      *
      * *
      */
    var pairOne = true
    val res = new ArrayBuffer[AlignmentRecord]()

    while (itr.hasNext) {
      if (counter == pairBound) {
        counter = 0
        pairOne = false
      }
      val entry = itr.next()
      if (pairOne) entry.setReadName(entry.getReadName + s" ${"%010d".format(counter * 2 + serialOffset)}")
      else entry.setReadName(entry.getReadName + s" ${"%010d".format(counter * 2 + 1 + serialOffset)}")
      counter += 1
      res.append(entry)
    }
    res.iterator
  }
}