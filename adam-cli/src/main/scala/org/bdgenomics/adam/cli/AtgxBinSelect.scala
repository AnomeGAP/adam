package org.bdgenomics.adam.cli

import org.apache.avro.Schema
import org.apache.avro.file.DataFileStream
import org.apache.avro.specific.{ SpecificDatumReader, SpecificRecordBase }
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.parquet.filter2.predicate.FilterApi.{ and, userDefined }
import org.apache.parquet.filter2.predicate.{ FilterApi, FilterPredicate, Statistics, UserDefinedPredicate }
import org.apache.spark.SparkContext
import org.bdgenomics.adam.cli.BinSelectType.BinSelectType
import org.bdgenomics.adam.ds.ADAMContext.sparkContextToADAMContext
import org.bdgenomics.adam.ds.read.AlignmentDataset
import org.bdgenomics.adam.models.{ ReadGroup, ReadGroupDictionary, SequenceDictionary }
import org.bdgenomics.formats.avro.{ Alignment, ProcessingStep, Reference, ReadGroup => RecordGroupMetadata }
import org.kohsuke.args4j.spi.{ Messages, OneArgumentOptionHandler, Setter }
import org.kohsuke.args4j.{ CmdLineException, CmdLineParser, OptionDef }
import org.seqdoop.hadoop_bam.SAMFormat

import java.io.InputStream
import java.util.concurrent.ForkJoinPool
import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.collection.parallel.ForkJoinTaskSupport
import scala.reflect.ClassTag
import scala.util.{ Failure, Success, Try }

object AtgxBinSelect {
  def runAgtxBinSelect(input: String, output: String, args: TransformAlignmentsArgs)(implicit sc: SparkContext): Unit = {
    val binSelect = new AtgxBinSelect(input, args.fileFormat, sc.hadoopConfiguration)
    args.selectType match {
      case BinSelectType.All =>
        binSelect.selectAll().saveAsSam(output, asType = binSelect.format, isSorted = true, asSingleFile = true)
      case BinSelectType.Unmap =>
        binSelect.selectUnmap().saveAsSam(output, asType = binSelect.format, isSorted = true, asSingleFile = true)
      case BinSelectType.ScOrdisc =>
        binSelect.selectScOrdisc().saveAsSam(output, asType = binSelect.format, isSorted = true, asSingleFile = true)
      case BinSelectType.UnmapAndScOrdisc =>
        binSelect.selectUnmapAndScOrdisc().saveAsSam(output, asType = binSelect.format, isSorted = true, asSingleFile = true)
      case BinSelectType.Select =>
        binSelect.select(args.dict, args.regions.asScala.toMap, args.bedAsRegions, args.poolSize)
          .foreach { i =>
            val ext = binSelect.ext
            val outputPath = List(output, ext, i._1 + "." + ext).mkString("/")
            // should not defer merging since we remove SeqPiper
            i._2.saveAsSam(outputPath, asType = binSelect.format, isSorted = true, asSingleFile = true)
          }
    }
  }
}

class AtgxBinSelect(input: String, fileFormat: String, hadoopConfig: Configuration) extends Serializable {
  val partitionSize: Int = 1000000
  lazy val (sd, rgd, pgs) = loadAvroDictionary(hadoopConfig, input)
  lazy val (ext, format) = getFileFormat(fileFormat, sd)

  def selectAll()(implicit sc: SparkContext): AlignmentDataset = {
    val rdd = sc.loadParquet[Alignment](fsWithPrefix("part-r-", input))
    AlignmentDataset(rdd, sd, rgd, pgs)
  }

  def selectUnmap()(implicit sc: SparkContext): AlignmentDataset = {
    val unmapPath = getUnmapPath(input)
    val rdd = sc.loadParquet[Alignment](unmapPath)
    AlignmentDataset(rdd, sd, rgd, pgs)
  }

  def selectScOrdisc()(implicit sc: SparkContext): AlignmentDataset = {
    val rdd = sc.loadParquet[Alignment](fsWithPrefix("X-SOFTCLIP-OR-DISCORDANT", input))
    AlignmentDataset(rdd, sd, rgd, pgs)
  }

  def selectUnmapAndScOrdisc()(implicit sc: SparkContext): AlignmentDataset = {
    val unmapPath = getUnmapPath(input)
    val rdd = sc.loadParquet[Alignment](unmapPath + "," + fsWithPrefix("X-SOFTCLIP-OR-DISCORDANT", input))
    AlignmentDataset(rdd, sd, rgd, pgs)
  }

  def select(dict: String,
             regions: Map[String, String],
             bedAsRegions: String,
             poolSize: Int)(implicit sc: SparkContext): List[(String, AlignmentDataset)] = {
    val (_, _, _, posBinIndices) = mkPosBinIndices(sd)

    // collect contigs from sequence dictionary
    val contigNames = sd.records.map(x => x.name.toLowerCase -> x.name).toMap
    // (k, v) => (0, chr1:0-13090000,chr1:13090000-29900000 || X-SOFTCLIP[:Start-End])

    val part = {
      if (bedAsRegions == "") null
      else
        GenomicPartitioner(
          new Path(bedAsRegions.split(",").head).getFileSystem(sc.hadoopConfiguration),
          bedAsRegions.split(",").map(x => new Path(x)),
          new Path(dict))
          .groupById()
    }

    val forkJoinPool = new ForkJoinPool(poolSize)
    val parallel_regions = if (part == null) regions.par else part.mapValues(_ => "").par
    parallel_regions.tasksupport = new ForkJoinTaskSupport(forkJoinPool)

    val result = parallel_regions.map {
      case (k, v) =>
        val regions =
          if (bedAsRegions != "") {
            part(k).toArray
          } else
            v.split(",")

        val files =
          if (v.startsWith("X-")) {
            fsWithPrefix(regions(0).split(":")(0), input)
          } else {
            collectParquetFileNames(input, regions, partitionSize, contigNames, posBinIndices)
          }

        val preds = regions.flatMap(mkRegionPredicates).reduceOption(FilterApi.or)
        val rdd = sc.loadParquet[Alignment](files, preds)
        k -> AlignmentDataset(rdd, sd, rgd, pgs)
    }.toList

    forkJoinPool.shutdown()
    result
  }

  private def loadAvro[T <: SpecificRecordBase](hadoopConfig: Configuration, filename: String, schema: Schema)(implicit tTag: ClassTag[T]): Seq[T] = {
    // get our current file system
    val path = new Path(filename)
    val fs = path.getFileSystem(hadoopConfig)

    // get an input stream
    val is = fs.open(path)
      .asInstanceOf[InputStream]

    // set up avro for reading
    val dr = new SpecificDatumReader[T](schema)
    val fr = new DataFileStream[T](is, dr)

    // get iterator and create an empty list
    val iter = fr.iterator
    var list = List.empty[T]
    while (iter.hasNext) {
      list = iter.next :: list
    }

    // close file
    fr.close()
    is.close()

    list
  }

  private def loadAvroDictionary(hadoopConfig: Configuration, input: String): (SequenceDictionary, ReadGroupDictionary, Seq[ProcessingStep]) = {
    val avroSd = loadAvro[Reference](hadoopConfig, input + "/_references.avro", Reference.SCHEMA$)
    val sd = SequenceDictionary.fromAvro(avroSd).sorted
    val avroRgd = loadAvro[RecordGroupMetadata](hadoopConfig, input + "/_readGroups.avro", RecordGroupMetadata.SCHEMA$)
    // convert avro to record group dictionary
    val rgd = new ReadGroupDictionary(avroRgd.map(ReadGroup.fromAvro))
    val pgs = loadAvro[ProcessingStep](hadoopConfig, input + "/_processingSteps.avro", ProcessingStep.SCHEMA$)
    (sd, rgd, pgs)
  }

  private def fsWithPrefix(prefix: String, input: String): String = {
    FileSystem
      .get(new Configuration)
      .globStatus(new Path(input + s"/$prefix*"))
      .map(x => x.getPath.toString)
      .mkString(",")
  }

  private def getUnmapPath(input: String): String = {
    // position bin is designed to partition genome into 1M bp bins to facilitate efficient bam entries
    // retrieval.
    // last 25 bins in part-r-xxxxx.snappy.parquet naming convention are used to store unmapped reads
    // retrieve unmapped reads therefrom
    FileSystem
      .get(new Configuration)
      .globStatus(new Path(input + "/part-*"))
      .map(x => x.getPath.toString)
      .takeRight(25)
      .mkString(",")
  }

  private def mkRegionPredicates(region: String): Option[FilterPredicate] = {
    // region format => chr:100-200 or X-...
    if (region.startsWith("X-")) {
      None
    } else {
      val items = region.split("[:-]")
      val lowest: java.lang.Long = items(1).toLong
      val highest: java.lang.Long = items(2).toLong
      Some(and(userDefined(FilterApi.longColumn("start"), new HighestFilter(highest)),
        userDefined(FilterApi.longColumn("end"), new LowestFilter(lowest))))
    }
  }

  private def collectParquetFileNames(input: String,
                                      regions: Array[String],
                                      partitionSize: Int,
                                      contigNames: Map[String, String],
                                      posBinIndices: Map[String, Vector[(Int, Int)]]): String = {
    regions
      .flatMap(v => {
        val items = v.split("[:-]")
        val nc = normalizeContig(items(0), contigNames)
        val low = scala.math.floor(items(1).toLong / partitionSize).toInt
        val high = scala.math.floor(items(2).toLong / partitionSize).toInt + 1
        posBinIndices(nc)
          .withFilter(bin => bin._1 >= low && bin._1 < high)
          .map(x => s"$input/part-r-${"%05d".format(x._2)}.snappy.parquet")
      })
      .distinct
      .mkString(",")
  }

  /**
   * Normalize the BED contig according to the SequenceDictionay to form the proper query.
   *
   * @param contig      contig from the BED
   * @param contigNames Lowercase names of SequenceDictionary as key, and original names as values
   * @return normalize contig
   */
  private def normalizeContig(contig: String, contigNames: Map[String, String]): String = {
    // `contig` variable must be in lower-case to be compared in map
    val lccontig = contig.toLowerCase()

    contigNames.get(lccontig) match {
      case Some(i) => i
      case None =>
        if (lccontig.startsWith("chr")) {
          if (lccontig == "chrm")
            contigNames.getOrElse[String]("mt", lccontig)
          else
            contigNames.getOrElse[String](lccontig.substring(3), lccontig)
        } else {
          if (lccontig == "mt")
            contigNames.getOrElse[String]("chrm", lccontig)
          else
            contigNames.getOrElse[String]("chr" + lccontig, lccontig)
        }
    }
  }

  def mkPosBinIndices(sd: SequenceDictionary, partitionSize: Int = 1000000): (Int, Int, Int, Map[String, Vector[(Int, Int)]]) = {
    val stopwords = Seq("chrU_", "chrUn_", "chrEBV", "_decoy", "_random", "_hap", "GL000", "NC_007605", "hs37d5",
      "CAST", "JH", "KB", "KK", "KQ", "KV", "MG", "PWK", "WSB")
    val filteredContigNames = sd.records
      .filterNot(x => stopwords.exists(x.name.contains))
      .sortBy(x => x.toADAMReference.getIndex.toInt)
      .map(x => {
        if (x.name.startsWith("HLA-")) "HLA=0"
        else if (x.name.endsWith("_alt")) "alt=0"
        else x.name + "=" + x.length
      })
      .distinct // distinct: deduplication of 'HLA=0' and 'alt=0'

    // when lots sequences with poor quality will excess the memory limit in the single partition,
    // here we separate those unmapped reads randomly
    val um = (0 to 24).map(i => "X-UNMAPPED@%05d=0".format(i))

    // duplicated reads => given-name_chromosome-index=num_bins
    // the num_bins = 0 stands for only one bin, 9M will created 10 bins (0-9)
    val sc = (0 to 24).map(i => "X-SOFTCLIP-OR-DISCORDANT@%05d=9000000".format(i))

    var ordBinCount = 0
    filteredContigNames.foreach(x => {
      for (_ <- 0 to scala.math.floor(x.split("=")(1).toLong / partitionSize).toInt)
        ordBinCount += 1
    })

    var unmapBinCount = 0
    um.foreach(x => {
      for (_ <- 0 to scala.math.floor(x.split("=")(1).toLong / partitionSize).toInt)
        unmapBinCount += 1
    })

    var scordisBinCount = 0
    sc.foreach(x => {
      for (_ <- 0 to scala.math.floor(x.split("=")(1).toLong / partitionSize).toInt)
        scordisBinCount += 1
    })

    (ordBinCount, unmapBinCount, scordisBinCount,
      (filteredContigNames ++ um ++ sc)
      .flatMap(
        x => {
          val buf = scala.collection.mutable.ArrayBuffer.empty[String]
          val t = x.split("=")
          for (numberOfPosBin <- 0 to scala.math.floor(t(1).toLong / partitionSize).toInt) {
            buf += t(0) + "@" + numberOfPosBin
          }
          buf
        })
      .zipWithIndex
      .map(x => {
        val y = x._1.split("@")
        (y(0), (y(1).toInt, x._2))
      })
      .groupBy(_._1)
      .mapValues(_.map(_._2)))
  }

  private def getFileFormat(fileFormat: String, sd: SequenceDictionary): (String, Option[SAMFormat]) = {
    if (fileFormat == "bam") {
      ("bam", Some(SAMFormat.BAM))
    } else if (fileFormat == "cram") {
      ("cram", Some(SAMFormat.CRAM))
    } else {
      inferFromMD5(sd)
    }
  }

  /**
   * Infer the SAMFormat and corresponding extension from MD5 column in SequenceDictionary.
   *
   * @param sd SequenceDictionary from `_seqdict.avro`
   * @return (bam|cram, SAMFormat.BAM|SAMFormat.CRAM)
   */
  private def inferFromMD5(sd: SequenceDictionary): (String, Option[SAMFormat]) = {
    val md5 = sd.records.forall(_.md5.isDefined)
    if (md5) ("cram", Some(SAMFormat.CRAM)) else ("bam", Some(SAMFormat.BAM))
  }

  private def mergePaths(pathes: String*): String = pathes.mkString("/")
}

class HighestFilter[T <: Comparable[T]](h: T) extends UserDefinedPredicate[T] with Serializable {
  override def inverseCanDrop(statistics: Statistics[T]): Boolean = !canDrop(statistics)

  override def canDrop(statistics: Statistics[T]): Boolean = statistics.getMin.compareTo(h) == 1

  override def keep(value: T): Boolean = value != null && (value.compareTo(h) <= 0)
}

class LowestFilter[T <: Comparable[T]](l: T) extends UserDefinedPredicate[T] with Serializable {
  override def inverseCanDrop(statistics: Statistics[T]): Boolean = !canDrop(statistics)

  override def canDrop(statistics: Statistics[T]): Boolean = statistics.getMax.compareTo(l) == -1

  override def keep(value: T): Boolean = value != null && (value.compareTo(l) >= 0)
}

object BinSelectType extends Enumeration {
  type BinSelectType = Value

  val All: BinSelectType.Value = Value("All")
  val Unmap: BinSelectType.Value = Value("Unmap")
  val ScOrdisc: BinSelectType.Value = Value("ScOrdisc")
  val UnmapAndScOrdisc: BinSelectType.Value = Value("UnmapAndScOrdisc")
  val Select: BinSelectType.Value = Value("Select")
}

class BinSelectSrcHandler(parser: CmdLineParser, option: OptionDef, setter: Setter[_ >: BinSelectType])
    extends OneArgumentOptionHandler[BinSelectType](parser, option, setter) {

  @throws[CmdLineException]
  protected def parse(argument: String): BinSelectType = {
    Try(BinSelectType.withName(argument)) match {
      case Success(v) => v
      case Failure(_) => throw new CmdLineException(owner, Messages.ILLEGAL_OPERAND, option.toString, argument)
    }
  }
}