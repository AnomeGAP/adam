package org.bdgenomics.adam.cli

import org.apache.spark.SparkContext
import org.bdgenomics.formats.avro.AlignmentRecord
import org.bdgenomics.adam.util.ArrayByteUtils._

import scala.collection.mutable.Queue
import scala.math.BigInt

object AtgxReadsBarcodeTrimmer {
  // store barcode via 2bit encoded
  // if barcode not in whitelist, then store 0(16 A's)
  final val UNKNOWN_BARCODE = 0
}

class AtgxReadsBarcodeTrimmer(sc: SparkContext, barcodeLen: Int, nMerLen: Int, whitelistPath: String) extends Serializable {
  val whitelist = sc.broadcast(sc.textFile(whitelistPath).map(i => seqToHash(i) -> BigInt(encode(i)._1).toInt).collect().toMap)
  val matchCnt = sc.longAccumulator("match_counter")
  val mismatchOneCnt = sc.longAccumulator("mismatch1_counter")
  val ambiguousCnt = sc.longAccumulator("ambiguous_counter")
  val unknownCnt = sc.longAccumulator("unknown_counter")

  def trim(iter: Iterator[AlignmentRecord]): Iterator[AlignmentRecord] = {
    val queue = new Queue[Int]
    iter.map { record =>
      val name = record.getReadName
      val (_, iw) = AtgxReadsInfoParser.parseFromName(name)
      if ((iw.getID & 0x1) == 0) {
        val (r1, code) = trimBarcode(record)
        queue.enqueue(code)
        r1
      } else {
        addBarcode(record, queue.dequeue())
      }
    }
  }

  def statistics(): Unit = {
    val total = matchCnt.value + mismatchOneCnt.value + ambiguousCnt.value + unknownCnt.value

    println(s"10x barcode match: ${matchCnt.value} ${matchCnt.value.toDouble / total * 100}")
    println(s"10x barcode mismatch1: ${mismatchOneCnt.value} ${mismatchOneCnt.value.toDouble / total * 100}")
    println(s"10x barcode ambiguous: ${ambiguousCnt.value} ${ambiguousCnt.value.toDouble / total * 100}")
    println(s"10x barcode unknown: ${unknownCnt.value} ${unknownCnt.value.toDouble / total * 100}")
  }

  private def trimBarcode(r1: AlignmentRecord): (AlignmentRecord, Int) = {
    val seq = r1.getSequence
    val quality = r1.getQual
    // create a new String to allow the original String to be GC
    val barcode = new String(seq.substring(0, barcodeLen))
    val code = matchWhitelist(barcode)

    r1.setSequence(new String(seq.substring(barcodeLen + nMerLen)))
    r1.setQual(new String(quality.substring(barcodeLen + nMerLen)))
    val (org, iw) = AtgxReadsInfoParser.parseFromName(r1.getReadName)
    iw.setBarcode(code)
    r1.setReadName(AtgxReadsInfoParser.updateName(org, iw))
    r1 -> code
  }

  private def addBarcode(r2: AlignmentRecord, barcode: Int): AlignmentRecord = {
    val (org, iw) = AtgxReadsInfoParser.parseFromName(r2.getReadName)
    iw.setBarcode(barcode)
    r2.setReadName(AtgxReadsInfoParser.updateName(org, iw))
    r2
  }

  private def matchWhitelist(barcode: String): Int = {
    val wl = whitelist.value
    val key = seqToHash(barcode)
    if (wl.contains(key)) {
      matchCnt.add(1)
      wl(key)
    } else {
      val hammingOne = getHammingOne(barcode)
      val hammingTest = hammingOne.map(wl.contains).map(i => if (i) 1 else 0)
      val result = hammingTest.sum

      result match {
        case 0 => {
          unknownCnt.add(1)
          AtgxReadsBarcodeTrimmer.UNKNOWN_BARCODE
        }
        case 1 => {
          val key = hammingTest.zip(hammingOne).find { case (r, _) => r == 1 }.get._2
          val correctedBarcode = wl(key)
          mismatchOneCnt.add(1)
          correctedBarcode
        }
        case _ => {
          ambiguousCnt.add(1)
          AtgxReadsBarcodeTrimmer.UNKNOWN_BARCODE
        }
      }
    }
  }

  // base on proc10xG
  // https://github.com/ucdavis-bioinformatics/proc10xG/blob/master/process_10xReads.py
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
    } yield new String(seq.substring(0, idx)) + base + new String(seq.substring(idx + 1))).map(seqToHash)
  }

  private def seqToHash(seq: String): Long = {
    // N defaults to A
    val encoding = Map('a' -> 0, 'c' -> 1, 'g' -> 2, 't' -> 3, 'A' -> 0, 'C' -> 1, 'G' -> 2, 'T' -> 3)
    seq.zipWithIndex.map { case (s, idx) => encoding.getOrElse(s, 0) * Math.pow(4, idx) }.sum.toLong
  }
}
