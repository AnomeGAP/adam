package org.bdgenomics.adam.cli

import org.apache.spark.SparkContext
import org.bdgenomics.utils.cli._
import org.kohsuke.args4j.spi.MapOptionHandler
import org.kohsuke.args4j.{Argument, Option => Args4jOption}

import scala.collection.JavaConverters.mapAsScalaMapConverter

object BinSelect extends BDGCommandCompanion {
  val commandName = "binSelect"
  val commandDescription = "Given BED and region, select corresponding BAM"

  def apply(cmdLine: Array[String]) = {
    new BinSelect(Args4j[BinSelectArgs](cmdLine))
  }
}

class BinSelectArgs extends Args4jBase {
  @Argument(required = true, metaVar = "INPUT", usage = "Parquet files folder path", index = 0)
  var inputPath: String = null

  @Argument(required = true, metaVar = "OUTPUT",
    usage = "Location to write selected BAM", index = 1)
  var outputPath: String = null

  @Args4jOption(required = true, name = "-atgx_bin_select", handler = classOf[BinSelectSrcHandler], usage = "select type: All, Unmap, ScOrdisc, UnmapAndScOrdisc, Select")
  var atgxBinSelect = BinSelectType.Select

  @Args4jOption(required = true, name = "-dict", usage = "dict path")
  var dict: String = ""

  @Args4jOption(required = true, name = "-l", usage = "One line for each genomic region", handler = classOf[MapOptionHandler])
  var regions: java.util.HashMap[String, String] = _

  @Args4jOption(required = false, name = "-bed_region", usage = "use bed as region input")
  var bedAsRegions: String = ""

  @Args4jOption(required = false, name = "-file_format", usage = "File formats for saving, e.g., bam or cram")
  var fileFormat: String = "bam"

  @Args4jOption(required = false, name = "-pool-size", usage = "# of parallel task")
  var poolSize: Int = 10
}

class BinSelect(val args: BinSelectArgs)
  extends BDGSparkCommand[BinSelectArgs] {

  val companion = BinSelect

  def run(sc: SparkContext) {
    val binSelect = new AtgxBinSelect(args.inputPath, args.outputPath, args.fileFormat, sc.hadoopConfiguration)
    args.atgxBinSelect match {
      case BinSelectType.All              => binSelect.selectAll()(sc)
      case BinSelectType.Unmap            => binSelect.selectUnmap()(sc)
      case BinSelectType.ScOrdisc         => binSelect.selectScOrdisc()(sc)
      case BinSelectType.UnmapAndScOrdisc => binSelect.selectUnmapAndScOrdisc()(sc)
      case BinSelectType.Select           => binSelect.select(args.dict, args.regions.asScala.toMap, args.bedAsRegions, args.poolSize)(sc)
    }
  }
}

