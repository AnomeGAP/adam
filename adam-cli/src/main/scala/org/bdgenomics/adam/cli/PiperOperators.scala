package org.bdgenomics.adam.cli

import cats.data.EitherT
import com.atgenomix.operators.{ GenericFormat, Partition, Source }
import net.general.piper.dsl.Dataset
import net.general.piper.dsl.Dataset.NopDataset
import net.general.piper.dsl.RddDataset.StringRddDataset
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.bdgenomics.adam.cli.AtgxTransformAlignments.{ mkPosBinIndices, renameWithXPrefix }
import org.bdgenomics.adam.ds.read.AlignmentDataset
import org.bdgenomics.adam.models.SequenceDictionary
import org.bdgenomics.formats.avro.Alignment
import org.seqdoop.hadoop_bam.SAMFormat
import utils.misc.AuditInfo

import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.reflect.{ ClassTag, classTag }

object PiperOperators {
  case class PiperAlignmentDataset(
      inputId: Int,
      rdd: RDD[Alignment],
      override val localPath: String,
      override val url: Option[String],
      alignmentDataset: Option[AlignmentDataset],
      args: TransformAlignmentsArgs,
      dict: SequenceDictionary,
      ctg: Option[String] = None,
      ext: Option[String] = None,
      format: Option[SAMFormat] = None) extends StringRddDataset(inputId, localPath, url) {
    override type T = Alignment
    override val ct: ClassTag[Alignment] = classTag[Alignment]
  }

  class ChunkedBamSource(
      override val inputId: Int,
      url: EitherT[Option, Seq[Seq[String]], Seq[String]],
      codec: Option[String],
      auth: String,
      localPath: EitherT[Option, Seq[Seq[String]], Seq[String]],
      override val extraInfo: Map[String, Any],
      override val auditInfo: AuditInfo) extends Source(inputId, url, codec, auth, localPath, extraInfo, auditInfo) {

    override def readImpl(url: String, local: String)(implicit spark: SparkSession): PiperAlignmentDataset = {
      val cmdLine = Seq(
        url,
        "", // we don't save file here so empty string for output is fine
        "-force_load_bam",
        "-atgx_transform",
        "-parquet_compression_codec",
        "SNAPPY")

      val args = org.bdgenomics.utils.cli.Args4j[TransformAlignmentsArgs](cmdLine.toArray)
      args.command = cmdLine.mkString(" ")
      println(args.command)
      val tra = new TransformAlignments(args)
      val (outputDs, originSd, _) = tra.init(spark.sparkContext)

      PiperAlignmentDataset(inputId, outputDs.rdd, local, Some(url), Some(outputDs), args, originSd)
    }
  }

  case class BamPartition(
      inputId: Int,
      parallelism: String,
      ref: Option[String] = None,
      extraInfo: Map[String, Any],
      auditInfo: AuditInfo) extends Partition(inputId, parallelism, ref, extraInfo, auditInfo) {

    override def partitionImpl(ds: Dataset)(implicit spark: SparkSession): List[Dataset] = {
      ds match {
        case p: PiperAlignmentDataset =>
          atgxTransform(p)
          binSelect(p)
        case _ =>
          throw new RuntimeException("Operator type mismatch")
      }
    }

    def atgxTransform(ds: PiperAlignmentDataset): Unit = {
      ds.args.outputPath = extraInfo.get("parquet-output")
        .map(_.asInstanceOf[String])
        .getOrElse(throw new RuntimeException("should specify `parquet-output`"))
      val args = ds.args
      val disableSVDup = args.disableSVDup
      val dict = mkPosBinIndices(ds.dict)
      val aDs = ds.alignmentDataset.getOrElse(throw new RuntimeException(""))

      val rdd = ds.rdd
        .mapPartitions(new AtgxTransformAlignments().transform(ds.dict, _, disableSVDup))
        .repartitionAndSortWithinPartitions(new NewPosBinPartitioner(dict))
        .map(_._2)

      AlignmentDataset(rdd, aDs.references, aDs.readGroups, aDs.processingSteps)
        .save(args, isSorted = args.sortByReadName || args.sortByReferencePosition || args.sortByReferencePositionAndIndex)

      renameWithXPrefix(args.outputPath, dict)
    }

    def binSelect(ds: PiperAlignmentDataset)(implicit spark: SparkSession): List[Dataset] = {
      val poolSize = extraInfo.get("pool-size").map(_.asInstanceOf[String]).getOrElse("10")
      val selectType = extraInfo.get("select-type").map(_.asInstanceOf[String]).getOrElse("Select")
      val region = extraInfo.get("region").map(_.asInstanceOf[String]).getOrElse("")
        .split(",")
        .flatMap(i => List("-l", i))
        .toSeq
      val cmdLine = Seq(
        ds.args.outputPath,
        "-select_type",
        selectType,
        "-dict",
        ref.get,
        "-bed_region",
        parallelism,
        "-pool-size",
        poolSize
      ) ++ region

      val args = org.bdgenomics.utils.cli.Args4j[TransformAlignmentsArgs](cmdLine.toArray)
      args.command = cmdLine.mkString(" ")
      println(args.command)
      implicit val sc: SparkContext = spark.sparkContext
      val binSelect = new AtgxBinSelect(args.inputPath, args.fileFormat, spark.sparkContext.hadoopConfiguration)
      BinSelectType.withName(selectType) match {
        case BinSelectType.All =>
          val aDs = binSelect.selectAll()
          List(ds.copy(alignmentDataset = Some(aDs), format = binSelect.format))
        case BinSelectType.Unmap =>
          val aDs = binSelect.selectUnmap()
          List(ds.copy(alignmentDataset = Some(aDs), format = binSelect.format))
        case BinSelectType.ScOrdisc =>
          val aDs = binSelect.selectScOrdisc()
          List(ds.copy(alignmentDataset = Some(aDs), format = binSelect.format))
        case BinSelectType.UnmapAndScOrdisc =>
          val aDs = binSelect.selectUnmapAndScOrdisc()
          List(ds.copy(alignmentDataset = Some(aDs), format = binSelect.format))
        case BinSelectType.Select =>
          binSelect.select(args.dict, args.regions.asScala.toMap, args.bedAsRegions, args.poolSize)
            .map(i => ds.copy(alignmentDataset = Some(i._2), ctg = Some(i._1), ext = Some(binSelect.ext), format = binSelect.format))
      }
    }
  }

  class PartitionedBamFormat(
      override val inputId: Int,
      url: EitherT[Option, Seq[Seq[String]], Seq[String]],
      auth: String,
      localPath: EitherT[Option, Seq[Seq[String]], Seq[String]],
      override val extraInfo: Map[String, Any],
      override val auditInfo: AuditInfo) extends GenericFormat(inputId, url, auth, localPath, extraInfo, auditInfo) {

    override def writeImpl(ds: Dataset, url: String)(implicit spark: SparkSession): Dataset = {
      ds match {
        case p: PiperAlignmentDataset =>
          p.alignmentDataset.foreach { i =>
            // in original BinSelect, we'll create a folder named by ext under url.
            // but we don't do that here
            val outputPath = p.ctg.map(c => List(url, c + "." + p.ext).mkString("/")).getOrElse(url)
            i.saveAsSam(outputPath, asType = p.format, isSorted = true, asSingleFile = true)
          }
        case _ => throw new RuntimeException("DSL err: should be StringRddDataset")
      }
      NopDataset()
    }
  }
}