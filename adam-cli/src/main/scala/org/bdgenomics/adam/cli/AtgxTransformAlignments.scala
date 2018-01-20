package org.bdgenomics.adam.cli

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.Partitioner
import org.bdgenomics.adam.models.SequenceDictionary
import org.bdgenomics.formats.avro.AlignmentRecord

object AtgxTransformAlignments {
  def mkPosBinIndices(sd: SequenceDictionary, partitionSize: Int = 1000000): Map[String, Int] = {
    val stopwords = Seq("chrU_", "chrUn_", "chrEBV", "_alt", "_decoy", "_random", "_hap", "GL000")
    val filteredContigNames = sd.records.filterNot(x => stopwords.exists(x.name.contains))
      .sortBy(x => x.referenceIndex.get)
      .map(x => if (x.name.startsWith("HLA-")) "HLA=0" else x.name + "=" + x.length)
      .distinct // distinct: deduplication of 'HLA=0'

    // duplicated reads => given-name_chromosome-index=num_bins
    // the num_bins = 0 stands for only one bin, 9M will created 10 bins (0-9)
    val dr = (0 to 24).map(i => "X-DISCORDANT_" + "%05d".format(i) + "=0")
    val sc = (0 to 24).map(i => "X-SOFTCLIP_" + "%05d".format(i) + "=9000000")

    (filteredContigNames ++ dr ++ sc)
      .flatMap(
        x => {
          val buf = scala.collection.mutable.ArrayBuffer.empty[String]
          val t = x.split("=")
          for (numberOfPosBin <- 0 to scala.math.floor(t(1).toLong / partitionSize).toInt) {
            buf += t(0) + "_" + numberOfPosBin
          }
          buf.iterator
        })
      .zipWithIndex
      .toMap
  }

  // X-UNMAPPED is not considered because it will be selected when select all parquets,
  // it needs to maintain the original naming such as `part-r-#####.snappy.parquet`.
  def renameWithXPrefix(path: String, dict: Map[String, Int]) {
    val conf: Configuration = new Configuration
    val fs: FileSystem = FileSystem.get(conf)

    dict.filterKeys(k => k.startsWith("X-DISCORDANT") || k.startsWith("X-SOFTCLIP"))
      .foreach(x => { // x => (String, Int)
        val pid = "%05d".format(x._2)
        val src = s"$path/part-r-$pid.snappy.parquet"
        val dst = s"$path/${x._1}-$pid.snappy.parquet"
        fs.rename(new Path(src), new Path(dst))
      })
  }
}

class AtgxTransformAlignments {
  // For the special partition, `X-SOFTCLIP`, it will generente a 4.9 G parquet
  // and cause the Executor Lost with our minimum resource setting. Also we cannot
  // apply the uniform bin-size (e.g., 1000000) for each reads due to produce the
  // uneven parquets. Hence, we make new boundaries for each contig then calculate
  // the bin-size according to its bound.
  //
  // +---------------------------------------------------+
  // |         |     hg19    |     hg38    | *           |
  // |---------|-------------|-------------|-------------|
  // |  chr1   |  249250621  |  248956422  |  250000000  |
  // |  chr2   |  243199373  |  242193529  |  244000000  |
  // |  chr3   |  198022430  |  198295559  |  199000000  |
  // |  chr4   |  191154276  |  190214555  |  192000000  |
  // |  chr5   |  180915260  |  181538259  |  182000000  |
  // |  chr6   |  171115067  |  170805979  |  172000000  |
  // |  chr7   |  159138663  |  159345973  |  160000000  |
  // |  chr8   |  146364022  |  145138636  |  147000000  |
  // |  chr9   |  141213431  |  138394717  |  142000000  |
  // |  chr10  |  135534747  |  133797422  |  136000000  |
  // |  chr11  |  135006516  |  135086622  |  136000000  |
  // |  chr12  |  133851895  |  133275309  |  134000000  |
  // |  chr13  |  115169878  |  114364328  |  116000000  |
  // |  chr14  |  107349540  |  107043718  |  108000000  |
  // |  chr15  |  102531392  |  101991189  |  103000000  |
  // |  chr16  |  90354753   |  90338345   |  91000000   |
  // |  chr17  |  81195210   |  83257441   |  84000000   |
  // |  chr18  |  78077248   |  80373285   |  81000000   |
  // |  chr19  |  59128983   |  58617616   |  60000000   |
  // |  chr20  |  63025520   |  64444167   |  65000000   |
  // |  chr21  |  48129895   |  46709983   |  49000000   |
  // |  chr22  |  51304566   |  50818468   |  52000000   |
  // |  chrX   |  155270560  |  156040895  |  157000000  |
  // |  chrY   |  59373566   |  57227415   |  60000000   |
  // |  chrM   |  16571      |  16569      |  17000      |
  // +---------------------------------------------------+
  def mkBinSizeMap(fold: Int = 10): Map[String, Int] = {
    Map(
      "chr1" -> 250000000, "chr2" -> 244000000, "chr3" -> 199000000,
      "chr4" -> 192000000, "chr5" -> 182000000, "chr6" -> 172000000, "chr7" -> 160000000,
      "chr8" -> 147000000, "chr9" -> 142000000, "chr10" -> 136000000, "chr11" -> 136000000,
      "chr12" -> 134000000, "chr13" -> 116000000, "chr14" -> 108000000, "chr15" -> 103000000,
      "chr16" -> 91000000, "chr17" -> 84000000, "chr18" -> 81000000, "chr19" -> 60000000,
      "chr20" -> 65000000, "chr21" -> 49000000, "chr22" -> 52000000, "chrX" -> 157000000,
      "chrY" -> 60000000, "chrM" -> 17000,
      "1" -> 250000000, "2" -> 244000000, "3" -> 199000000,
      "4" -> 192000000, "5" -> 182000000, "6" -> 172000000, "7" -> 160000000,
      "8" -> 147000000, "9" -> 142000000, "10" -> 136000000, "11" -> 136000000,
      "12" -> 134000000, "13" -> 116000000, "14" -> 108000000, "15" -> 103000000,
      "16" -> 91000000, "17" -> 84000000, "18" -> 81000000, "19" -> 60000000,
      "20" -> 65000000, "21" -> 49000000, "22" -> 52000000, "X" -> 157000000,
      "Y" -> 60000000, "MT" -> 17000
    )
      .mapValues(v => v / fold)
  }

  // sort the contigs by its ReferenceIndex in SequenceDirectory then zip them with index
  def mkReferenceIdMap(sd: SequenceDictionary): Map[String, Int] = {
    val ref: Map[String, Int] = sd.records.map(x => (x.name, x.referenceIndex.get)).toMap.withDefaultValue(10000)
    val ucsc = (1 to 22).map(i => "chr" + i) ++ Seq("chrX", "chrY", "chrM")
    val grch37 = (1 to 22).map(_.toString) ++ Seq("X", "Y", "MT")
    (ucsc.map(x => (x, ref(x))).sortBy(_._2).map(_._1).zipWithIndex ++
      grch37.map(x => (x, ref(x))).sortBy(_._2).map(_._1).zipWithIndex).toMap
  }

  def transform(sd: SequenceDictionary, iter: Iterator[AlignmentRecord]): Iterator[(String, AlignmentRecord)] = {
    val X_UNMAPPED_PARTITION_NAME = "X-UNMAPPED"
    val partitionSize: Int = 1000000
    val binSizeMap = mkBinSizeMap()
    val map = mkReferenceIdMap(sd)
    val refIndexMap = sd.records.map(x => (x.name, "%05d".format(x.referenceIndex.get))).toMap
    val words = Seq("chrU_", "chrUn_", "chrEBV", "_alt", "_decoy", "_random", "_hap", "GL000", "hs37d5")

    val buf = scala.collection.mutable.ArrayBuffer.empty[(String, AlignmentRecord)]
    while (iter.hasNext) {
      val x = iter.next()
      if (x.getReadMapped == false) { // unmapped reads
        buf += ((">" + X_UNMAPPED_PARTITION_NAME + "_" + X_UNMAPPED_PARTITION_NAME + "000000000", x))
      } else {
        val contigName = x.getContigName
        if (!words.exists(contigName.contains)) { // filter out the unused records
          val posBin = scala.math.floor(x.getStart / partitionSize).toInt
          val paddingStart = "%09d".format(x.getStart.toInt)
          val ci = refIndexMap(contigName)

          if (contigName.startsWith("HLA")) {
            buf += ((ci + ">" + contigName + "_" + "%05d".format(posBin) + "=" + paddingStart, x))
          } else {
            // primary assembly
            buf += ((ci + ">" + contigName + "_" + posBin + "=" + paddingStart, x))
            // make duplication of the following cases: X-DISCORDANT, X-SOFTCLIP
            val paddedChromosomeIndex = "%05d".format(map(contigName))
            if (x.getProperPair == false) {
              // divided by chromosome, so the the index of bin always be 0
              buf += ((ci + ">" + "X-DISCORDANT_" + paddedChromosomeIndex + "_0=" + paddingStart, x))
            }
            if (x.getCigar.contains("S")) {
              val bin = x.getStart / binSizeMap(contigName)
              buf += ((ci + ">" + "X-SOFTCLIP_" + paddedChromosomeIndex + "_" + bin + "=" + paddingStart, x))
            }
          }
        }
      }
    }

    buf.iterator
  }
}

class NewPosBinPartitioner(dict: Map[String, Int]) extends Partitioner {
  override def numPartitions: Int = dict.size + 1 // null is the last one partition

  val X_UNMAPPED_PARTITION_NAME = "X-UNMAPPED"

  override def getPartition(key: Any): Int = key match {
    case key: String =>
      // format => contigNameIndex__contigName_posBin=paddingStart
      val c = key.split("=")(0).split(">")(1)
      if (c.startsWith("HLA")) {
        dict("HLA_0")
      } else if (c.startsWith(X_UNMAPPED_PARTITION_NAME)) {
        numPartitions - 1
      } else {
        dict(c)
      }
  }
}
