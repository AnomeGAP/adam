package org.bdgenomics.adam.cli

import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.spark.Partitioner

import scala.annotation.tailrec
import scala.collection.mutable
import scala.io.Source
import scala.util.Sorting

object GenomicPartitioner {
  val P_MULTIPLY = 10000000000000L
  val C_MULTIPLY = 1000000000L

  def apply(fs: FileSystem, path: Array[Path], sd: Path): GenomicPartitioner = {
    val part = new GenomicPartitioner()
    path.foreach(pp => {
      val bed = fs.open(pp)
      var auto: Long = 1
      Source
        .fromInputStream(bed)
        .getLines
        .withFilter(_.length > 0)
        .withFilter(!_.startsWith("#"))
        .foreach(s => {
          val r = s.split("\t")
          if (r.length >= 5) {
            if (r(1).toInt - r(4).toInt < 0) {
              part.put(r(0), 0, r(2).toInt + r(4).toInt, r(3).toLong)
            } else {
              part.put(r(0), r(1).toInt - r(4).toInt, r(2).toInt + r(4).toInt, r(3).toLong)
            }
          } else if (r.length >= 4) {
            part.put(r(0), r(1).toInt, r(2).toInt, r(3).toLong)
          } else {
            part.put(r(0), r(1).toInt, r(2).toInt, auto)
            auto += 1
          }
        })
      bed.close()
    })
    part.build(fs, sd)
  }
}

class GenomicPartitioner extends Partitioner {

  /**
   * Models a full-closed interval [start, end]
   */
  case class Interval(start: Int, end: Int) {
    require(start <= end)
    def overlaps(r: Interval): Boolean = start <= r.end && r.start <= end
    def contains(x: Int): Boolean = start <= x && x <= end
    def lessthan(x: Int): Boolean = x > end
    def greaterthan(x: Int): Boolean = x < start
    def compare(r: Interval): Int = if (start < r.start) -1 else if (start > r.start) 1 else 0
    override def toString = s"[$start, $end]"
  }

  /**
   * Pair of position interval and its partition identifier
   */
  case class IntervalPair(int: Interval, part: Long) {
    override def toString: String = int.toString + ":" + part.toString
  }

  object IntervalPairOrdering extends Ordering[IntervalPair] {
    def compare(a: IntervalPair, b: IntervalPair): Int = a.int.compare(b.int)
  }

  /**
   * The partition map by contig name as key
   */
  private[this] val intervals = mutable.HashMap.empty[String, Array[IntervalPair]]
  /**
   * The sequence dictionary
   */
  private[this] val seqdict = mutable.HashMap.empty[String, Long]

  def put(ctg: String, start: Int, end: Int, part: Long): Any = {
    intervals.get(ctg) match {
      case Some(a) => intervals.put(ctg, a :+ IntervalPair(Interval(start, end), part))
      case None    => intervals.put(ctg, Array(IntervalPair(Interval(start, end), part)))
    }
  }

  def printIntervals() = {
    intervals.foreach(x => {
      println("ctg " + x._1)
      x._2.foreach(y => println(y.toString))
    })
  }

  def build(fs: FileSystem, sd: Path): this.type = {
    intervals.foreach(x => Sorting.quickSort(x._2)(IntervalPairOrdering))
    val dict = fs.open(sd)
    var id: Long = 1
    Source
      .fromInputStream(dict)
      .getLines
      .withFilter(_.startsWith("@SQ"))
      .foreach(s => {
        s.split("\t").find(_.startsWith("SN:")) match {
          case Some(kv) => seqdict.put(kv.split(":")(1), id)
          case None     =>
        }
        id += 1
      })
    dict.close()
    this
  }

  /**
   * The method provides a utility function
   */
  def groupById(): Map[String, Iterable[String]] = {
    intervals
      .map(x => x._2.map(y => (y.part.toString, s"${x._1}:${y.int.start}-${y.int.end}")))
      .flatten
      .groupBy(_._1)
      .map(x => (x._1, x._2.map(_._2)))
  }

  @tailrec
  private def queryOrNone(p: Int, a: Array[IntervalPair], b: Int, e: Int): Option[Long] = {
    val m = (b + e) / 2
    if (b == e) {
      if (a(m).int.contains(p)) Some(a(m).part) else None
    } else if (a(m).int.contains(p)) {
      Some(a(m).part)
    } else if (a(m).int.lessthan(p)) {
      queryOrNone(p, a, m + 1, e)
    } else if (a(m).int.greaterthan(p)) {
      queryOrNone(p, a, b, m - 1)
    } else {
      None
    }
  }

  @tailrec
  private def queryMultiple(p: Int, a: Array[IntervalPair], b: Int, e: Int): Option[Array[Long]] = {
    val m = (b + e) / 2
    if (b >= e) {
      if (a(m).int.contains(p)) Some(Array(a(m).part)) else None
    } else if (a(m).int.contains(p)) {
      if (m > b && a(m - 1).int.contains(p)) Some(Array(a(m - 1).part, a(m).part))
      else if (m < e && a(m + 1).int.contains(p)) Some(Array(a(m).part, a(m + 1).part))
      else Some(Array(a(m).part))
    } else if (a(m).int.lessthan(p)) {
      queryMultiple(p, a, m + 1, e)
    } else if (a(m).int.greaterthan(p)) {
      queryMultiple(p, a, b, m - 1)
    } else {
      None
    }
  }

  def getKeyValOrNone(s: String, f: (String) => (String, Int)): Array[(Long, String)] = {
    val r = f(s)
    // don't do contig toLowerCase to deal with contig name like corona virus
    val c = r._1
    intervals.get(c) match {
      case Some(a) =>
        queryMultiple(r._2.toInt, a, 0, a.length - 1) match {
          case Some(v) => v.map(x => (x * GenomicPartitioner.P_MULTIPLY +
            seqdict.getOrElse(c, 0L) * GenomicPartitioner.C_MULTIPLY + r._2.toLong, s))
          case None => Array.empty
        }
      case None =>
        def _keyify3(k: String): Array[(Long, String)] = {
          intervals.get(k) match {
            case Some(a) =>
              queryMultiple(r._2.toInt, a, 0, a.length - 1) match {
                case Some(v) => v.map(x => (x * GenomicPartitioner.P_MULTIPLY +
                  seqdict.getOrElse(c, 0L) * GenomicPartitioner.C_MULTIPLY + r._2.toLong, s))
                case None => Array.empty
              }
            case None => Array.empty
          }
        }

        if (c.startsWith("chr")) {
          if (c == "chrM") _keyify3("MT")
          else _keyify3(c.substring(3))
        } else {
          if (c == "MT") _keyify3("chrM")
          else _keyify3("chr" + c)
        }
    }
  }

  def maxPartitionId: Long = intervals.flatMap(_._2).map(_.part).toSet.max

  override def numPartitions: Int = intervals.flatMap(_._2).map(_.part).toSet.size // + 1

  override def getPartition(key: Any): Int = (key.asInstanceOf[Long] / GenomicPartitioner.P_MULTIPLY).toInt

  override def toString: String = intervals.map(x => s"${x._1}->(${x._2.map(_.toString).mkString(",")})").mkString("\n")
}
