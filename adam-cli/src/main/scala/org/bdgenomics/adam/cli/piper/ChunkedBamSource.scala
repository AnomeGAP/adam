package org.bdgenomics.adam.cli.piper

import cats.data.EitherT
import com.atgenomix.operators.Source
import org.apache.spark.sql.SparkSession
import org.bdgenomics.adam.cli.{ TransformAlignments, TransformAlignmentsArgs }
import utils.misc.AuditInfo

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

    PiperAlignmentDataset(inputId, outputDs.rdd.map(PiperAlignmentDataset.EnhancedAlignment), local, Some(url), Some(outputDs), args, originSd)
  }
}
