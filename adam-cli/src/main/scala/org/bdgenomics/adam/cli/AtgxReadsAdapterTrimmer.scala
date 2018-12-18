package org.bdgenomics.adam.cli

import org.bdgenomics.formats.avro.AlignmentRecord

class AtgxReadsAdapterTrimmer {
  def trim(iter: Iterator[AlignmentRecord]): Iterator[AlignmentRecord] = {
    val (read1, read2) = iter.partition{ record =>
      val name = record.getReadName
      val (_, iw) = AtgxReadsInfoParser.parseFromName(name)
      // use readId to identify read1 read2
      if ((iw.getID & 0x1) == 0)
        true
      else
        false
    }

    val iter1 = read1.map { record =>
      val seq = record.getSequence
      val dist = AtgxReadsAdapterTrimmer.calculateADistance(seq)
      val firstAToHeadDist = seq.indexOf('A')

      dist -> firstAToHeadDist
    }

    val iter2 = read2.map { record =>
      // TODO: reverse complement
      val seq = record.getSequence
      val dist = AtgxReadsAdapterTrimmer.calculateADistance(seq)
      val lastAToEndDist = seq.length - seq.lastIndexOf('A') - 1

      dist -> lastAToEndDist
    }

    val overlapLen = iter1 zip iter2 map { case (it1, it2) =>
      val common = AtgxReadsAdapterTrimmer.longestCommonSubLists(it1._1, it2._1)
      it1._2 + common.sum + it2._2
    }

    iter zip (overlapLen ++ overlapLen) map { case (record, len) =>
      val newSeq = new String(record.getSequence.substring(0, len))
      val newQuality = new String(record.getQual.substring(0, len))
      record.setSequence(newSeq)
      record.setQual(newQuality)

      record
    }
  }
}

object AtgxReadsAdapterTrimmer {
  def calculateADistance(seq: String): List[Int] = {
    val idxSeq = seq.toList.zipWithIndex.filter(_._1 == 'A').map(_._2)
    val s1 = idxSeq.tail
    val s2 = idxSeq.dropRight(1)

    s1 zip s2 map { case (a, b) => a - b}
  }

  //  def getAllSubLists(xs: List[Int]): Set[List[Int]] = {
  //    xs.inits.flatMap(_.tails).toSet
  //  }
  //
  def longestCommonSubLists(ls1: List[Int], ls2: List[Int]): List[Int] = {
    val ls1Inits = ls1.inits.toSet
    val ls2Tails = ls2.tails.toSet

    ls1Inits.intersect(ls2Tails).maxBy(_.length)
  }
}
