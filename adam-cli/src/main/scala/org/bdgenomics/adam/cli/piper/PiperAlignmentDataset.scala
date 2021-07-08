package org.bdgenomics.adam.cli.piper

import net.general.piper.dsl.RddDataset.{ BaseStringContent, StringRddDataset }
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.cli.piper.PiperAlignmentDataset.EnhancedAlignment
import org.bdgenomics.adam.ds.read.AlignmentDataset
import org.bdgenomics.adam.models.SequenceDictionary
import org.bdgenomics.formats.avro.Alignment
import org.seqdoop.hadoop_bam.SAMFormat

import scala.reflect.{ ClassTag, classTag }

object PiperAlignmentDataset {

  implicit class EnhancedAlignment(val alignment: Alignment) extends BaseStringContent {
    def toStringContent: String = alignment.getSequence
  }
}

case class PiperAlignmentDataset(
    inputId: Int,
    rdd: RDD[EnhancedAlignment],
    override val localPath: String,
    override val url: Option[String],
    alignmentDataset: Option[AlignmentDataset],
    dict: SequenceDictionary,
    part: Option[String] = None,
    ext: Option[String] = None,
    format: Option[SAMFormat] = None) extends StringRddDataset(inputId, localPath, url) {
  override type T = EnhancedAlignment
  override val ct: ClassTag[EnhancedAlignment] = classTag[EnhancedAlignment]
}
