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

  // -1 for depth 0, which won't happen
  // 0 for depth 1-2
  // 1 for depth 3-7
  // 2 for depth 8-17
  // 3 for depth 18-37
  // 4 for depth 38-77
  // 5 for depth 78-157
  // 6 for depth 158-317
  val dLevel: Array[Int] = Array[Int](
    -1,

         0, 0,

         1, 1, 1, 1, 1,

         2, 2, 2, 2, 2, 2, 2, 2, 2, 2,

         3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
         3, 3, 3, 3, 3, 3, 3, 3, 3, 3,

         4, 4, 4, 4, 4, 4, 4, 4, 4, 4,
         4, 4, 4, 4, 4, 4, 4, 4, 4, 4,
         4, 4, 4, 4, 4, 4, 4, 4, 4, 4,
         4, 4, 4, 4, 4, 4, 4, 4, 4, 4,

         5, 5, 5, 5, 5, 5, 5, 5, 5, 5,
         5, 5, 5, 5, 5, 5, 5, 5, 5, 5,
         5, 5, 5, 5, 5, 5, 5, 5, 5, 5,
         5, 5, 5, 5, 5, 5, 5, 5, 5, 5,
         5, 5, 5, 5, 5, 5, 5, 5, 5, 5,
         5, 5, 5, 5, 5, 5, 5, 5, 5, 5,
         5, 5, 5, 5, 5, 5, 5, 5, 5, 5,
         5, 5, 5, 5, 5, 5, 5, 5, 5, 5,

         6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
         6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
         6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
         6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
         6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
         6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
         6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
         6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
         6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
         6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
         6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
         6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
         6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
         6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
         6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
         6, 6, 6, 6, 6, 6, 6, 6, 6, 6
    )

  val decodeDepth: Array[Int] = Array[Int](
    0, 1, 2, 3, 4, 5, 6,
         0, 1, 2, 3, 4, 5, 6,
         0, 1, 2, 3, 4, 5, 6,
         0, 1, 2, 3, 4, 5, 6,
         0, 1, 2, 3, 4, 5, 6,
         0, 1, 2, 3, 4, 5, 6
  )


  val qualMin = 33

  val qualMax = 74

  val depthMax = 318

  def collapse(rdd: RDD[AlignmentRecord]): RDD[AlignmentRecord] = {
    rdd
      .flatMap(
        fw => {
          val rc = new AlignmentRecord
          rc.setSequence(reverseComplementary(fw.getSequence))
          val (_, iw) = AtgxReadsInfoParser.parseFromName(fw.getReadName)
          ArrayBuffer[(Boolean, Long, AlignmentRecord)](
            (false, iw.getID(), fw),
            (true, iw.getID(), rc))
        })
      .keyBy(x => x._3.getSequence)
      .aggregateByKey(List.empty[(Boolean, Long, AlignmentRecord)])({ case (r, c) => c :: r }, { case (r, c) => c ::: r })
      .map(minByWithCount)
      .flatMap(
        x => {
          if (!x._1)
            Some(encodeQual(x._2, x._3))
          else
            None
        })
  }

  def minByWithCount(x: (String, List[(Boolean, Long, AlignmentRecord)])): (Boolean, AlignmentRecord, Int) = {
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
    (x._2(index)._1, x._2(index)._3, count)
  }

  // convert Illumina 1.8+ Phred+33 quality score, with value range of 33-74, to depth encoded value
  def encodeQual(item: AlignmentRecord, depth: Int): AlignmentRecord = {
    item.setQual(
//      s"@@$depth@@"
      item.getQual
        .map(
          x => {
            if (x.toInt > qualMax || x.toInt < qualMin)
              throw new Exception("given quality does not follow Illumina 1.8+ Phred+33 quality score format")
            (qLevel(x.toInt - qualMin) + dLevel(math.min(depth, depthMax))).toChar
//            dLevel(math.min(depth, 314)).toChar
          })
    )
    item
  }

  def decodeQual(item: AlignmentRecord): (String, Array[Int]) = {
    (item.getQual.map(x => { ( qLevel(x.toInt - qualMin) + qualMin ).toChar }),
      item.getQual.map(x => { dLevel(x.toInt - qualMin) }).toArray)
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
