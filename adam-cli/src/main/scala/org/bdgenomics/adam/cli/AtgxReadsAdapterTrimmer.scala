package org.bdgenomics.adam.cli

import org.bdgenomics.formats.avro.AlignmentRecord
import org.bdgenomics.adam.cli.Utils.reverseComplementary

import scala.collection.mutable.ListBuffer

class AtgxReadsAdapterTrimmer {
  def trim(iter: Iterator[AlignmentRecord]): Iterator[AlignmentRecord] = {
    val (it1, it2) = iter.duplicate
    val (read1, read2) = it1.partition { record =>
      val name = record.getReadName
      val (_, iw) = AtgxReadsInfoParser.parseFromName(name)
      // use readId to identify read1 read2
      if ((iw.getID & 0x1) == 0)
        true
      else
        false
    }
    // use the position of A to represent the sequence
    // the reason to use A is A has higher precision in Illumina 2-color chemistry
    val read1APos = read1.map { record =>
      val seq = record.getSequence
      val firstAToHeadDist = seq.indexOf('A')

      if (firstAToHeadDist == -1) {
        None
      } else {
        Some(calcRelativeDist(seq, 'A') -> firstAToHeadDist)
      }
    }

    val read2APos = read2.map { record =>
      val rcSeq = reverseComplementary(record.getSequence)
      val lastIdxOfA = rcSeq.lastIndexOf('A')
      if (lastIdxOfA == -1) {
        None
      } else {
        val lastAToEndDist = rcSeq.length - lastIdxOfA - 1
        Some(calcRelativeDist(rcSeq, 'A') -> lastAToEndDist)
      }
    }

    val (overlapLen1, overlapLen2) = (read1APos zip read2APos map {
      case (Some(r1APos), Some(r2APos)) => {
        val common = longestCommonSubLists(r1APos._1, r2APos._1)
        r1APos._2 + common.sum + r2APos._2
      }
      case _ => 0
    }).duplicate

    it2 zip (overlapLen1 ++ overlapLen2) map {
      case (record, len) =>
        if (len == 0) {
          record
        } else {
          val newSeq = new String(record.getSequence.substring(0, len))
          val newQuality = new String(record.getQual.substring(0, len))
          record.setSequence(newSeq)
          record.setQual(newQuality)

          record
        }
    }
  }

  private def calcRelativeDist(seq: String, nuc: Char): List[Int] = {
    val idxSeq = seq.toList.zipWithIndex.filter(_._1 == nuc).map(_._2)

    if (idxSeq.nonEmpty) {
      idxSeq.tail.foldLeft(ListBuffer[Int]() -> idxSeq.head) {
        case ((result, previous), current) =>
          val diff = current - previous
          (result += diff) -> current
      }._1.toList
    } else {
      List.empty
    }
  }

  // find longest common sublists with overlap is negative:
  //               |----read 1----->
  //         <-----read 2-------|
  private def longestCommonSubLists(ls1: List[Int], ls2: List[Int]): List[Int] = {
    val ls1Inits = ls1.inits.toSet
    val ls2Tails = ls2.tails.toSet

    ls1Inits.intersect(ls2Tails).maxBy(_.length)
  }
}
