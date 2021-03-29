package org.bdgenomics.adam.cli

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.cli.Utils.reverseComplementary
import org.bdgenomics.formats.avro.Alignment
import scala.collection.mutable.ArrayBuffer

class AtgxReadsDupCollapse extends java.io.Serializable {
  val qLevel: Array[Int] = Array[Int](
    33, 33, 33, 33, 33, 33, 33, 33, 33, 33, 33, 33, 33, 33,
    47, 47, 47, 47, 47, 47, 47, 47, 47, 47, 47, 47, 47, 47,
    61, 61, 61, 61, 61, 61, 61, 61, 61, 61, 61, 61, 61, 61)
  val depthEncoder = (d: Int) => if (d >= 14) 14 else d
  val qualMin = 33
  val qualMax = 74

  def collapse(rdd: RDD[Alignment]): RDD[Alignment] = {
    rdd
      .flatMap(
        fw => {
          val rc = new Alignment
          rc.setSequence(reverseComplementary(fw.getSequence))
          rc.setQualityScores(fw.getQualityScores.reverse)
          val (_, iw) = AtgxReadsInfoParser.parseFromName(fw.getReadName)
          ArrayBuffer[(Boolean, Long, Alignment)](
            (false, iw.getID, fw),
            (true, iw.getID, rc))
        })
      .keyBy(x => x._3.getSequence)
      .aggregateByKey(List.empty[(Boolean, Long, Alignment)])({ case (r, c) => c :: r }, { case (r, c) => c ::: r })
      .filter { case (_, list) => !list.minBy(_._2)._1 } // keep list that alignment record having min ID is not RC
      .map {
        case (_, list) =>
          val depth = list.size
          val encodedDepth = depthEncoder(depth)
          val quals = list.map(_._3.getQualityScores.toList)
          val len = list.head._3.getQualityScores.length
          val bestQual = chooseBestQual(quals, len - 1, List())
            .map { q =>
              if (q > qualMax || q < qualMin)
                throw new Exception("given quality does not follow Illumina 1.8+ Phred+33 quality score format")
              (qLevel(q - qualMin) + encodedDepth - 1).toChar
            }
            .mkString
          val min = list.minBy(_._2)._3
          min.setQualityScores(bestQual)
          min
      }
  }

  def chooseBestQual(chars: List[List[Char]], idx: Int, accum: List[Char]): List[Int] = {
    if (idx < 0) {
      accum.map(_.toInt)
    } else {
      val bestQual = chars.map(c => c(idx)).max
      chooseBestQual(chars, idx - 1, bestQual :: accum)
    }
  }
}

