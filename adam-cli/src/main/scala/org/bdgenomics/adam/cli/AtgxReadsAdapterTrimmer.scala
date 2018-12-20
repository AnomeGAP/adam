package org.bdgenomics.adam.cli

import org.bdgenomics.formats.avro.AlignmentRecord
import org.bdgenomics.adam.cli.Utils.reverseComplementary

import scala.collection.mutable.{HashMap, ListBuffer}

class AtgxReadsAdapterTrimmer {
  def trim(iter: Iterator[AlignmentRecord]): Iterator[AlignmentRecord] = {
    val unpairedReads = new HashMap[Long, AlignmentRecord]()
    iter.flatMap { record =>
      val name = record.getReadName
      val (_, iw) = AtgxReadsInfoParser.parseFromName(name)
      val readId = iw.getID
      val anotherPairId = if ((readId & 0x1) == 0) readId + 1 else readId - 1

      unpairedReads.get(anotherPairId)
        .map { another =>
          unpairedReads.remove(anotherPairId)
          // use the position of A to represent the sequence
          // the reason to use A is A has higher precision in Illumina 2-color chemistry
          val read1APos = getRead1APos(record)
          val read2APos = getRead2APos(another)
          val overlapLen = (read1APos, read2APos) match {
            case (Some(r1APos), Some(r2APos)) => {
              val common = longestCommonSubLists(r1APos._1, r2APos._1)
              if (common.isEmpty) None else Some(r1APos._2 + common.sum + r2APos._2 + 1)
            }
            case _ => None
          }

          overlapLen.map { len =>
            val trimmedRecord = trimAdapter(record, len)
            val trimmedAnother = trimAdapter(another, len)
            if (trimmedRecord.getSequence.compareTo(reverseComplementary(trimmedAnother.getSequence)) == 0)
              List(trimmedRecord, trimmedAnother)
            else
              List(record, another)
          }
          .getOrElse { List(record, another) }
        }
        .getOrElse {
          unpairedReads.put(readId, record)
          List.empty
        } ++ unpairedReads.values
    }
  }

  private def getRead1APos(record: AlignmentRecord): Option[(List[Int], Int)] = {
    val seq = record.getSequence
    val firstAToHeadDist = seq.indexOf('A')

    if (firstAToHeadDist == -1) {
      None
    } else {
      Some(calcRelativeDist(seq, 'A') -> firstAToHeadDist)
    }
  }

  private def getRead2APos(record: AlignmentRecord): Option[(List[Int], Int)] = {
    val rcSeq = reverseComplementary(record.getSequence)
    val lastIdxOfA = rcSeq.lastIndexOf('A')
    if (lastIdxOfA == -1) {
      None
    } else {
      val lastAToEndDist = rcSeq.length - lastIdxOfA - 1
      Some(calcRelativeDist(rcSeq, 'A') -> lastAToEndDist)
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

  private def trimAdapter(record: AlignmentRecord, len: Int): AlignmentRecord = {
    val newSeq = new String(record.getSequence.substring(0, len))
    val newQuality = new String(record.getQual.substring(0, len))
    record.setSequence(newSeq)
    record.setQual(newQuality)

    record
  }
}
