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
  val whitelist = sc.broadcast(sc.textFile(whitelistPath).map(i => seqToHash(i) -> BigInt(encode(i)._1).toInt).collect().toMap)
  val matchCnt = sc.longAccumulator("match_counter")
  val mismatchOneCnt = sc.longAccumulator("mismatch1_counter")
  val ambiguousCnt = sc.longAccumulator("ambiguous_counter")
  val unknownCnt = sc.longAccumulator("unknown_counter")

  def trim(iter: Iterator[AlignmentRecord], partitionSerialOffset: Int = 268435456): Iterator[AlignmentRecord] = {
    val list = iter.toList
    val length = list.length
    val read1 = list.slice(0, length / 2)
    val read2 = list.slice(length / 2, length)
    (read1 zip read2).flatMap { case (r1, r2) => trimmer(r1, r2) }
      .toIterator
  }

  def statistics(): Unit = {
    val total = matchCnt.value + mismatchOneCnt.value + ambiguousCnt.value + unknownCnt.value

    println(s"10x barcode match: ${matchCnt.value} ${matchCnt.value.toDouble / total * 100}")
    println(s"10x barcode mismatch1: ${mismatchOneCnt.value} ${mismatchOneCnt.value.toDouble / total * 100}")
    println(s"10x barcode ambiguous: ${ambiguousCnt.value} ${ambiguousCnt.value.toDouble / total * 100}")
    println(s"10x barcode unknown: ${unknownCnt.value} ${unknownCnt.value.toDouble / total * 100}")
  }

  private def trimmer(r1: AlignmentRecord, r2: AlignmentRecord): List[AlignmentRecord] = {
    val EXTEND = 0xFFFFFFFFFFFFFFFFL
    val seq = r1.getSequence
    val barcode = seq.substring(0, barcodeLen)
    val (code, result) = matchWhitelist(barcode)
    val encodedBarcode = code & EXTEND & result

    r1.setSequence(seq.substring(barcodeLen + nMerLen))
    r1.setReadName(r1.getReadName + " " + encodedBarcode)
    r2.setReadName(r2.getReadName + " " + encodedBarcode)
    List(r1, r2)
  }

  private def matchWhitelist(barcode: String): (Int, Long) = {
    val wl = whitelist.value
    val key = seqToHash(barcode)
    if (wl.contains(key)) {
      matchCnt.add(1)
      wl(key) -> AtgxBarcodeTrimmer.MATCH
    } else {
      val hammingOne = getHammingOne(barcode)
      val hammingTest = hammingOne.map(wl.contains).map(i => if (i) 1 else 0)
      val result = hammingTest.sum

      result match {
        case 0 => {
          unknownCnt.add(1)
          BigInt(encode(barcode)._1).toInt -> AtgxBarcodeTrimmer.UNKNOWN
        }
        case 1 => {
          val key = hammingTest.zip(hammingOne).find { case (r, _) => r == 1 }.get._2
          val correctedBarcode = wl(key)
          mismatchOneCnt.add(1)
          correctedBarcode -> AtgxBarcodeTrimmer.MISMATCH1
        }
        case _ => {
          ambiguousCnt.add(1)
          BigInt(encode(barcode)._1).toInt -> AtgxBarcodeTrimmer.AMBIGUOUS
        }
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
