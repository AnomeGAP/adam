package org.bdgenomics.adam.cli
import org.bdgenomics.formats.avro.Alignment
import scala.annotation.switch

class AtgxReadsLCFilter extends java.io.Serializable {
  // return tuple of (Iterator(ordinary reads), Iterator(low complexity reads)
  def filterReads(iter: Iterator[Alignment], invFlag: Boolean = false, kmer: Int, thresholdLenFactor: Int): Iterator[Alignment] = {
    val q =
      if (!invFlag)
        iter.filter(x => !isLC(x.getSequence, kmer, thresholdLenFactor))
      else
        iter.filter(x => isLC(x.getSequence, kmer, thresholdLenFactor))
    q
  }

  def nuc2num(c: Char): Int = {
    val num: Int = (c: @switch) match {
      case 'A' => 0
      case 'C' => 1
      case 'G' => 2
      case 'T' => 3
    }
    num
  }

  def kmer2Index(str: String): Int = {
    var sum = 0
    for (i <- 0 until str.length)
      sum += math.pow(4, i).toInt * nuc2num(str(i))
    sum
  }
  //TODO: reuse existence kmer index
  def scanKmer(str: String, kmer: Int, threshold: Int): Boolean = {
    val halfLen = str.length / 2
    val ofw = new Array[Int](math.pow(4, kmer).toInt)
    for (i <- 0 until halfLen)
      ofw(kmer2Index(str.substring(i, i + kmer))) += 1

    if (ofw.max > threshold)
      true
    else
      false
  }

  //TODO: in polyA scenario, kmer AAA will reach threshold 25 when the actual sequence includes 27 polyA.  should we take it as LC regions ?
  //TODO: calculate entropy
  // identify reads with single nucleotides as low complexity reads
  def isLC(str: String, kmer: Int, thresholdLenFactor: Int): Boolean = {
    val thresholdLen = str.length / thresholdLenFactor
    val threshold = thresholdLen / kmer
    if (scanKmer(str, kmer, threshold))
      true
    else
      scanKmer(str.reverse, kmer, threshold)
  }

}
