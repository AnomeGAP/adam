package org.bdgenomics.adam.cli

import org.apache.spark.rdd.RDD
import org.bdgenomics.formats.avro.AlignmentRecord
import scala.collection.mutable.ArrayBuffer

class AtgxReadsDupCollapse extends java.io.Serializable {
  var readName = ""
  var serialNumber = 0L
  var barcode = 0L
  //TODO: binary search => array
  val qLevel: Array[Int] = Array[Int](0, 33, 40, 47, 54, 61, 68, 75)
  val dLevel: Array[Int] = Array[Int](0, 5, 15, 35, 75, 155, 315, Int.MaxValue)

  def collapse(rdd: RDD[AlignmentRecord]): RDD[AlignmentRecord] = {
    val g = rdd
      .flatMap(
        fw => {
          val rc = new AlignmentRecord
          rc.setSequence(reverseComplementary(fw.getSequence))
          ArrayBuffer[(Boolean, Long, AlignmentRecord)](
            (false, fw.getReadName.split(" ")(1).toLong, fw),
            (true, fw.getReadName.split(" ")(1).toLong, rc))
        })
      //TODO: try aggregate by key
      .keyBy(x => x._3.getSequence)
      .groupByKey
      .map(x => (x._2.minBy(z => z._2), x._2.toArray.length))
      .flatMap(x => {
        val ret = ArrayBuffer.empty[AlignmentRecord]
        if (!x._1._1)
          ret.append(x._1._3)
        ret
      })
    g
  }

  // convert Illumina 1.8+ Phred+33 quality score, with value range of 33-74, to depth encoded value
  def encodeQual(item: AlignmentRecord, depth: Int): AlignmentRecord = {
    item.setQual(
      item.getQual
        .map(
          x => {
            if (x.toInt > 74 || x.toInt < 33)
              throw new Exception("given quality does not follow Illumina 1.8+ Phred+33 quality score format")
            (
              qLevel(binarySearch(x.toInt, qLevel, 0, qLevel.length)) +
              dLevel(binarySearch(depth, dLevel, 0, dLevel.length))
            )
              .toChar
          })
    )
    item
  }

  def binarySearch(query: Int, target: Array[Int], start: Int, end: Int): Int = {
    val diff: Int = (end - start) / 2
    val middle = diff + start
    if (query < target(middle)) {
      binarySearch(query, target, start, middle)
    } else if (query > target(middle + 1)) {
      binarySearch(query, target, middle + 1, end)
    } else
      middle
  }

  def reverseComplementary(s: String): String = {
    val complementary = Map('A' -> 'T', 'T' -> 'A', 'C' -> 'G', 'G' -> 'C')
    var i = 0
    var j = s.length - 1
    val c = s.toCharArray

    while (i < j) {
      val temp = c(i)
      c(i) = complementary(c(j))
      c(j) = complementary(temp)
      i = i + 1
      j = j - 1
    }

    if (s.length % 2 != 0) c(i) = complementary(c(i))

    String.valueOf(c)
  }
}
