package org.bdgenomics.adam.cli

import org.apache.spark.SparkContext
import org.bdgenomics.formats.avro.AlignmentRecord
import org.bdgenomics.adam.util.ArrayByteUtils._

import scala.io.Source
import scala.math.BigInt

object AtgxBarcodeTrimmer {
  final val MATCH = 0x0000000FFFFFFFFFL
  final val MISMATCH1 = 0x0000001FFFFFFFFFL
  final val AMBIGUOUS = 0x0000010FFFFFFFFFL
  final val UNKNOWN = 0x0000011FFFFFFFFFL
}

class AtgxBarcodeTrimmer(sc: SparkContext, barcodeLen: Int, nMerLen: Int, barcodeWhitelist: String) {
  val source = Source.fromFile(barcodeWhitelist)
  val whitelist = try { source.getLines.toList.map(i => seqToHash(i) -> i).toMap } finally source.close()
  val matchCnt = sc.longAccumulator("match_counter")
  val mismatchOneCnt = sc.longAccumulator("mismatch1_counter")
  val ambiguousCnt = sc.longAccumulator("ambiguous_counter")
  val unknownCnt = sc.longAccumulator("unknown_counter")

  def trim(iter: Iterator[AlignmentRecord], partitionSerialOffset: Int = 268435456): Iterator[AlignmentRecord] = {
    iter.map(trimmer)
  }

  def statistics(): Unit = {
    val total = matchCnt.value + mismatchOneCnt.value + ambiguousCnt.value + unknownCnt.value

    println(s"10x barcode match: ${matchCnt.value.toDouble / total * 100}")
    println(s"10x barcode mismatch1: ${mismatchOneCnt.value.toDouble / total * 100}")
    println(s"10x barcode ambiguous: ${ambiguousCnt.value.toDouble / total * 100}")
    println(s"10x barcode unknown: ${unknownCnt.value.toDouble / total * 100}")
  }

  private def trimmer(record: AlignmentRecord): AlignmentRecord = {
    val seq = record.getSequence
    val barcode = seq.substring(0, barcodeLen)
    val matchResult = matchWhitelist(barcode)
    val encodedBarcode = BigInt(encode(matchResult._1)._1).toLong & matchResult._2

    record.setSequence(seq.substring(barcodeLen + nMerLen))
    record.setReadName(record.getReadName + " " + encodedBarcode)
    record
  }

  private def matchWhitelist(barcode: String): (String, Long) = {
    if (whitelist.contains(seqToHash(barcode))) {
      matchCnt.add(1)
      barcode -> AtgxBarcodeTrimmer.MATCH
    } else {
      val hammingOne = getHammingOne(barcode)
      val hammingTest = hammingOne.map(whitelist.contains).map(i => if (i) 1 else 0)
      val result = hammingTest.sum

      if (result == 0) {
        unknownCnt.add(1)
        barcode -> AtgxBarcodeTrimmer.UNKNOWN
      } else if (result == 1) {
        val idx = hammingTest.zip(hammingOne).find { case (r, _) => r == 1 }.get._2
        val correctedBarcode = whitelist(idx)
        mismatchOneCnt.add(1)
        correctedBarcode -> AtgxBarcodeTrimmer.MISMATCH1
      } else {
        mismatchOneCnt.add(1)
        barcode -> AtgxBarcodeTrimmer.AMBIGUOUS
      }
    }
  }

  private def getHammingOne(seq: String): List[Long] = {
    val baseDict = Map(
      'A' -> List('C', 'G', 'T'),
      'C' -> List('A', 'G', 'T'),
      'G' -> List('A', 'C', 'T'),
      'T' -> List('A', 'C', 'G'),
      'N' -> List('A', 'C', 'G', 'T'),
      'a' -> List('C', 'G', 'T'),
      'c' -> List('A', 'G', 'T'),
      'g' -> List('A', 'C', 'T'),
      't' -> List('A', 'C', 'G'),
      'n' -> List('A', 'C', 'G', 'T'))

    (for {
      (b, idx) <- seq.zipWithIndex.toList
      base <- baseDict(b)
    } yield seq.substring(0, idx) + base + seq.substring(idx + 1)).map(seqToHash)
  }

  private def seqToHash(seq: String): Long = {
    val encoding = Map('a' -> 0, 'c' -> 1, 'g' -> 2, 't' -> 3, 'A' -> 0, 'C' -> 1, 'G' -> 2, 'T' -> 3)
    seq.map(i => encoding.getOrElse(i, 0) * Math.pow(4, i)).sum.toLong
  }
}
