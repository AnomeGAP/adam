package org.bdgenomics.adam.cli

import org.apache.spark.SparkContext
import org.bdgenomics.formats.avro.AlignmentRecord
import org.bdgenomics.adam.util.ArrayByteUtils._

import scala.math.BigInt

object AtgxBarcodeTrimmer {
  final val MATCH = 0x0000000FFFFFFFFFL
  final val MISMATCH1 = 0x0000001FFFFFFFFFL
  final val AMBIGUOUS = 0x0000010FFFFFFFFFL
  final val UNKNOWN = 0x0000011FFFFFFFFFL
}

class AtgxBarcodeTrimmer(sc: SparkContext, barcodeLen: Int, nMerLen: Int, whitelistPath: String) extends Serializable {
  val whitelist = sc.broadcast(sc.textFile(whitelistPath).map(i => seqToHash(i) -> i).collect().toMap)
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
    val EXTEND = 0xFFFFFFFFFFFFFFFFL
    val seq = record.getSequence
    val barcode = seq.substring(0, barcodeLen)
    val (code, result) = matchWhitelist(barcode)
    val encodedBarcode = BigInt(encode(code)._1).toInt & EXTEND & result

    record.setSequence(seq.substring(barcodeLen + nMerLen))
    record.setReadName(record.getReadName + " " + encodedBarcode)
    record
  }

  private def matchWhitelist(barcode: String): (String, Long) = {
    val wl = whitelist.value
    if (wl.contains(seqToHash(barcode))) {
      matchCnt.add(1)
      barcode -> AtgxBarcodeTrimmer.MATCH
    } else {
      val hammingOne = getHammingOne(barcode)
      val hammingTest = hammingOne.map(wl.contains).map(i => if (i) 1 else 0)
      val result = hammingTest.sum

      if (result == 0) {
        unknownCnt.add(1)
        barcode -> AtgxBarcodeTrimmer.UNKNOWN
      } else if (result == 1) {
        val key = hammingTest.zip(hammingOne).find { case (r, _) => r == 1 }.get._2
        val correctedBarcode = wl(key)
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
    // N defaults to A
    val encoding = Map('a' -> 0, 'c' -> 1, 'g' -> 2, 't' -> 3, 'A' -> 0, 'C' -> 1, 'G' -> 2, 'T' -> 3)
    seq.zipWithIndex.map { case (s, idx) => encoding.getOrElse(s, 0) * Math.pow(4, idx) }.sum.toLong
  }
}
