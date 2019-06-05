package org.bdgenomics.adam.cli

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.cli.Utils.reverseComplementary
import org.bdgenomics.formats.avro.AlignmentRecord
import scala.collection.mutable.ArrayBuffer

class AtgxReadsDupCollapse extends java.io.Serializable {
  val qLevel: Array[Int] = Array[Int](
    33, 33, 33, 33, 33, 33, 33, 33, 33, 33, 33, 33, 33, 33,
    47, 47, 47, 47, 47, 47, 47, 47, 47, 47, 47, 47, 47, 47,
    61, 61, 61, 61, 61, 61, 61, 61, 61, 61, 61, 61, 61, 61)
  val depthEncoder = (d: Int) => if (d >= 14) 14 else d
  val qualMin = 33
  val qualMax = 74

  def collapse(rdd: RDD[AlignmentRecord]): RDD[AlignmentRecord] = {
    rdd
      .flatMap(
        fw => {
          val rc = new AlignmentRecord
          rc.setSequence(reverseComplementary(fw.getSequence))
          rc.setQual(fw.getQual.reverse)
          val (_, iw) = AtgxReadsInfoParser.parseFromName(fw.getReadName)
          ArrayBuffer[(Boolean, Long, AlignmentRecord)](
            (false, iw.getID, fw),
            (true, iw.getID, rc))
        })
      .keyBy(x => x._3.getSequence)
      .aggregateByKey(List.empty[(Boolean, Long, AlignmentRecord)])({ case (r, c) => c :: r }, { case (r, c) => c ::: r })
      .filter { case (_, list) => !list.minBy(_._2)._1 } // keep list that alignment record having min ID is not RC
      .map {
        case (_, list) =>
          val depth = list.size
          val encodedDepth = depthEncoder(depth)
          val qual = list.map(_._3.getQual.toList)
          // get the best quality for each position in read
          val encodedQual = qual.transpose
            .map { i =>
              val max = i.max
              if (max.toInt > qualMax || max.toInt < qualMin)
                throw new Exception("given quality does not follow Illumina 1.8+ Phred+33 quality score format")
              max
            }
            // encode the quality and add depth info
            .map(i => (qLevel(i.toInt - qualMin) + encodedDepth).toChar)
            .mkString
          val min = list.minBy(_._2)._3
          min.setQual(encodedQual)
          min
      }
  }
}

