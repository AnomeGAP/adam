package org.bdgenomics.adam.cli.piper

import cats.data.EitherT
import com.atgenomix.operators.GenericFormat
import net.general.piper.dsl.Dataset
import net.general.piper.dsl.Dataset.NopDataset
import org.apache.spark.sql.SparkSession
import org.bdgenomics.adam.cli.piper.Util.gen2ToHdfs
import utils.misc.AuditInfo

import java.util.concurrent.ForkJoinPool
import scala.collection.parallel.ForkJoinTaskSupport

class PartitionedBamFormat(
    override val inputId: Int,
    url: EitherT[Option, Seq[Seq[String]], Seq[String]],
    auth: String,
    localPath: EitherT[Option, Seq[Seq[String]], Seq[String]],
    override val extraInfo: Map[String, Any],
    override val auditInfo: AuditInfo) extends GenericFormat(inputId, url, auth, localPath, extraInfo, auditInfo) {

  override def writeDatasets(ds: List[Dataset])(implicit spark: SparkSession): List[Dataset] = {
    val poolSize = extraInfo.get("pool-size").map(_.asInstanceOf[String]).getOrElse("10").toInt
    val dst = url.fold(_.flatten, identity)
      .filter(_.nonEmpty)
      .getOrElse(throw new RuntimeException("DSL err: url should not be empty"))
      .head

    val forkJoinPool = new ForkJoinPool(poolSize)
    val parallelDs = ds.par
    parallelDs.tasksupport = new ForkJoinTaskSupport(forkJoinPool)
    val result = parallelDs.map { d => writeImpl(d, dst) }.toList
    forkJoinPool.shutdown()
    result
  }

  override def writeImpl(ds: Dataset, url: String)(implicit spark: SparkSession): Dataset = {
    ds match {
      case p: PiperAlignmentDataset =>
        p.alignmentDataset.foreach { i =>
          // in original BinSelect, we'll create a folder named by ext under url.
          // but we don't do that here
          val outputPath = p.part.map(c => List(url, c + "." + p.ext.get).mkString("/")).getOrElse(url)
          i.saveAsSam(gen2ToHdfs(outputPath), asType = p.format, isSorted = true, asSingleFile = true)
        }
      case _ => throw new RuntimeException("DSL err: should be StringRddDataset")
    }
    NopDataset()
  }
}
