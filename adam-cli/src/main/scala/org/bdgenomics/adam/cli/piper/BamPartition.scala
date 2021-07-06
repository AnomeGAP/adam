package org.bdgenomics.adam.cli.piper

import com.atgenomix.operators.Partition
import net.general.piper.dsl.Dataset
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.bdgenomics.adam.cli.AtgxTransformAlignments.{ mkPosBinIndices, renameWithXPrefix }
import org.bdgenomics.adam.cli._
import org.bdgenomics.adam.ds.read.AlignmentDataset
import utils.misc.AuditInfo

import scala.collection.JavaConverters.mapAsScalaMapConverter

case class BamPartition(
    inputId: Int,
    parallelism: String,
    ref: Option[String] = None,
    extraInfo: Map[String, Any],
    auditInfo: AuditInfo) extends Partition(inputId, parallelism, ref, extraInfo, auditInfo) {

  override def partitionImpl(ds: Dataset)(implicit spark: SparkSession): List[Dataset] = {
    ds match {
      case p: PiperAlignmentDataset =>
        val parquetOutput = atgxTransform(p)
        binSelect(p, parquetOutput)
      case _ =>
        throw new RuntimeException("Operator type mismatch")
    }
  }

  def atgxTransform(ds: PiperAlignmentDataset): String = {
    val output = ds.url.get.stripSuffix("/") + ".parquet"
    val cmdLine = Array(ds.url.get, output, "-parquet_compression_codec", "SNAPPY")
    val args = org.bdgenomics.utils.cli.Args4j[TransformAlignmentsArgs](cmdLine)
    val disableSVDup = args.disableSVDup
    val dict = mkPosBinIndices(ds.dict)
    val aDs = ds.alignmentDataset.getOrElse(throw new RuntimeException(""))

    val rdd = ds.rdd
      .map(_.alignment)
      .mapPartitions(new AtgxTransformAlignments().transform(ds.dict, _, disableSVDup))
      .repartitionAndSortWithinPartitions(new NewPosBinPartitioner(dict))
      .map(_._2)
    AlignmentDataset(rdd, aDs.references, aDs.readGroups, aDs.processingSteps)
      .save(args, isSorted = args.sortByReadName || args.sortByReferencePosition || args.sortByReferencePositionAndIndex)
    renameWithXPrefix(args.outputPath, dict)

    args.outputPath
  }

  def binSelect(ds: PiperAlignmentDataset, parquetPath: String)(implicit spark: SparkSession): List[Dataset] = {
    val poolSize = extraInfo.get("pool-size").map(_.asInstanceOf[String]).getOrElse("10")
    val selectType = extraInfo.get("select-type").map(_.asInstanceOf[String]).getOrElse("Select")
    val region = extraInfo.get("region").map(_.asInstanceOf[String]).getOrElse(",")
      .split(",")
      .flatMap(i => List("-l", i))
      .toSeq
    val cmdLine = Seq(
      parquetPath,
      "", // just for TransformAlignmentsArgs validation
      "-bam_output", // just for TransformAlignmentsArgs validation
      "", // just for TransformAlignmentsArgs validation
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
        val regions = if (args.regions == null) Map.empty[String, String] else args.regions.asScala.toMap
        binSelect.select(args.dict, regions, args.bedAsRegions, args.poolSize)
          .map(i => ds.copy(alignmentDataset = Some(i._2), ctg = Some(i._1), ext = Some(binSelect.ext), format = binSelect.format))
    }
  }
}
