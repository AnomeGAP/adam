package org.bdgenomics.adam.cli

import org.bdgenomics.formats.avro.AlignmentRecord
import org.bdgenomics.adam.cli.Utils.reverseComplementary

import scala.annotation.tailrec
import scala.collection.mutable.HashMap

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
          val (read1APos, read2APos) = if (readId < anotherPairId) {
            getAPos(record.getSequence) -> getAPos(reverseComplementary(another.getSequence))
          } else {
            getAPos(another.getSequence) -> getAPos(reverseComplementary(record.getSequence))
          }
          val overlapLen = (read1APos, read2APos) match {
            case (Some(r1APos), Some(r2APos)) => {
              val common = longestCommonSubLists(r1APos._1, r2APos._1)
              // the second condition is to make sure read1's prefix overlaps read2's postfix:
              //               |----read 1----->
              //         <-----read 2-------|
              if (common.isEmpty || r1APos._3 < r2APos._3)
                None
              else Some(r1APos._2 + common.sum + r2APos._3 + 1)
            }
            case _ => None
          }

          overlapLen.map { len =>
            if (len > record.getSequence.length / 3) {
              val trimmedRecord = trimAdapter(record, len)
              val trimmedAnother = trimAdapter(another, len)
              if (checkDiff(trimmedRecord.getSequence, trimmedAnother.getSequence))
                List(trimmedRecord, trimmedAnother)
              else
                List(record, another)
            } else
              List(record, another)
          }
            .getOrElse { List(record, another) }
        }
        .getOrElse {
          unpairedReads.put(readId, record)
          List[AlignmentRecord]()
        }
    } ++ unpairedReads.values
  }

  private def getAPos(seq: String, nuc: Char = 'A'): Option[(List[Int], Int, Int)] = {
    var idx = 0
    var firstAIdx = -1
    var lastAIdx = -1
    var previousAIdx = -1

    val aPos = seq.map { c =>
      if (c == nuc) {
        val t = if (firstAIdx == -1) {
          firstAIdx = idx
          -1
        } else {
          idx - previousAIdx
        }
        previousAIdx = idx
        lastAIdx = idx
        idx += 1
        t
      } else {
        idx += 1
        -1
      }
    }
      .filter(_ >= 0)

    if (firstAIdx != -1) {
      Some((aPos.toList, firstAIdx, seq.length - lastAIdx - 1))
    } else {
      None
    }
  }

  // find longest common sublists with overlap is negative or 0(perfectly overlap):
  //               |----read 1----->
  //         <-----read 2-------|
  //
  //               |----read 1----->
  //               <----read 2-----|
  private def longestCommonSubLists(ls1: List[Int], ls2: List[Int]): List[Int] = {
    val ls1Inits = ls1.inits
    val ls2Tails = ls2.tails

    // ls1Inits.intersect(ls2Tails).maxBy(_.length)
    lcsAux(ls1Inits, ls2Tails)
  }

  @tailrec
  private def lcsAux(ls1: Iterator[List[Int]], ls2: Iterator[List[Int]]): List[Int] = {
    (ls1.hasNext, ls2.hasNext) match {
      case (true, true) => {
        val h1 = ls1.next()
        val h2 = ls2.next()
        if (h1 == h2) {
          h1
        } else {
          if (h1.length >= h2.length)
            lcsAux(ls1, Iterator(h2) ++ ls2)
          else
            lcsAux(Iterator(h1) ++ ls1, ls2)
        }
      }
      case _ => List.empty
    }
  }

  private def checkDiff(seq1: String, seq2: String, maxDiff: Int = 5): Boolean = {
    val map = Map('A' -> 1, 'C' -> 2, 'T' -> 3, 'G' -> 4)
    val diff = seq1 zip seq2 count {
      case (c1, c2) =>
        if ((map(c1) ^ map(c2)) == 0)
          false
        else
          true
    }

    if (diff <= maxDiff)
      true
    else
      false
  }

  private def trimAdapter(record: AlignmentRecord, len: Int): AlignmentRecord = {
    val newSeq = new String(record.getSequence.substring(0, len))
    val newQuality = new String(record.getQuality.substring(0, len))
    record.setSequence(newSeq)
    record.setQuality(newQuality)

    record
  }
}
