package org.bdgenomics.adam.cli

import org.apache.spark.rdd.RDD
import org.bdgenomics.formats.avro.AlignmentRecord
import scala.collection.mutable.ArrayBuffer

class AtgxReadsDupCollapse extends java.io.Serializable {
  val qLevel: Array[Int] = Array[Int](
    33, 33, 33, 33, 33, 33, 33,
         40, 40, 40, 40, 40, 40, 40,
         47, 47, 47, 47, 47, 47, 47,
         54, 54, 54, 54, 54, 54, 54,
         61, 61, 61, 61, 61, 61, 61,
         68, 68, 68, 68, 68, 68, 68)

  val dLevel: Array[Int] = Array[Int](
    0, 0, 0, 0, 0, 

         5, 5, 5, 5, 5, 5, 5, 5, 5, 5,

         15, 15, 15, 15, 15, 15, 15, 15, 15, 15,
         15, 15, 15, 15, 15, 15, 15, 15, 15, 15,

         35, 35, 35, 35, 35, 35, 35, 35, 35, 35,
         35, 35, 35, 35, 35, 35, 35, 35, 35, 35,
         35, 35, 35, 35, 35, 35, 35, 35, 35, 35,
         35, 35, 35, 35, 35, 35, 35, 35, 35, 35,

         75, 75, 75, 75, 75, 75, 75, 75, 75, 75,
         75, 75, 75, 75, 75, 75, 75, 75, 75, 75,
         75, 75, 75, 75, 75, 75, 75, 75, 75, 75,
         75, 75, 75, 75, 75, 75, 75, 75, 75, 75,
         75, 75, 75, 75, 75, 75, 75, 75, 75, 75,
         75, 75, 75, 75, 75, 75, 75, 75, 75, 75,
         75, 75, 75, 75, 75, 75, 75, 75, 75, 75,
         75, 75, 75, 75, 75, 75, 75, 75, 75, 75,

         155, 155, 155, 155, 155, 155, 155, 155, 155, 155, 
         155, 155, 155, 155, 155, 155, 155, 155, 155, 155, 
         155, 155, 155, 155, 155, 155, 155, 155, 155, 155, 
         155, 155, 155, 155, 155, 155, 155, 155, 155, 155, 
         155, 155, 155, 155, 155, 155, 155, 155, 155, 155, 
         155, 155, 155, 155, 155, 155, 155, 155, 155, 155, 
         155, 155, 155, 155, 155, 155, 155, 155, 155, 155, 
         155, 155, 155, 155, 155, 155, 155, 155, 155, 155, 
         155, 155, 155, 155, 155, 155, 155, 155, 155, 155, 
         155, 155, 155, 155, 155, 155, 155, 155, 155, 155, 
         155, 155, 155, 155, 155, 155, 155, 155, 155, 155, 
         155, 155, 155, 155, 155, 155, 155, 155, 155, 155, 
         155, 155, 155, 155, 155, 155, 155, 155, 155, 155, 
         155, 155, 155, 155, 155, 155, 155, 155, 155, 155, 
         155, 155, 155, 155, 155, 155, 155, 155, 155, 155, 
         155, 155, 155, 155, 155, 155, 155, 155, 155, 155
    )

  def collapse(rdd: RDD[AlignmentRecord]): RDD[AlignmentRecord] = {
    rdd
      .flatMap(
        fw => {
          val rc = new AlignmentRecord
          rc.setSequence(reverseComplementary(fw.getSequence))
          ArrayBuffer[(Boolean, Long, AlignmentRecord)](
            (false, fw.getReadName.split(" ")(1).toLong, fw),
            (true, fw.getReadName.split(" ")(1).toLong, rc))
        })
      .keyBy(x => x._3.getSequence)
      .aggregateByKey(List.empty[(Boolean, Long, AlignmentRecord)])({ case (r, c) => c :: r }, { case (r, c) => c ::: r })
      .map(minByWithCount)
      //      .map(x => (x._2.minBy(z => z._2), x._2.toArray.length))
      .flatMap(
        x => {
          if (!x._1._1)
            Some(x._1._3)
          else
            None
    })
  }

  def minByWithCount(x: (String, List[(Boolean, Long, AlignmentRecord)])): ((Boolean, Long, AlignmentRecord), Int) = {
    var min = Long.MaxValue
    var count = 0
    var index = 0
    for (i <- x._2.indices) {
      if (x._2(i)._2 < min) {
        min = x._2(i)._2
        index = i
      }
      count += 1
    }
    (x._2(index), count)
  }

  // convert Illumina 1.8+ Phred+33 quality score, with value range of 33-74, to depth encoded value
  def encodeQual(item: AlignmentRecord, depth: Int): AlignmentRecord = {
    item.setQual(
      item.getQual
        .map(
          x => {
            if (x.toInt > 74 || x.toInt < 33)
              throw new Exception("given quality does not follow Illumina 1.8+ Phred+33 quality score format")
            (qLevel(x.toInt) + dLevel(math.min(depth, 314))).toChar
          })
    )
    item
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
