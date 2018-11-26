package org.bdgenomics.adam.cli

import org.apache.spark.TaskContext
import org.bdgenomics.formats.avro.AlignmentRecord

import scala.collection.mutable.HashSet


object AtgxReadsInfoParser {
  def parseFromName(nameStr: String): (String, AtgxReadsInfoWrapper) = {
    val pos = nameStr.lastIndexOf(" ")
    if (pos == -1)
      (nameStr, new AtgxReadsInfoWrapper(""))
    else
      (nameStr.substring(0, pos), new AtgxReadsInfoWrapper(nameStr.substring(pos + 1)))
  }

  def updateName(nameStr: String, infoWrapper: AtgxReadsInfoWrapper): String = {
    nameStr + infoWrapper.toString()
  }
}

class AtgxReadsInfoWrapper {
  /**
    * This class parse readName field of AlignmentRecord, and provide getter and setter interfaces for
    * accessing Atgenomix information
   */
  private var _ID: Long = 0L
  private var _barcode: Int = 0
  private var _SN: String = "-"

  def this(str: String) = {
    this()
    val c = str.split("#")
    if (c.length >= 3) {
      this._ID = c(0).toLong
      this._barcode = c(1).toInt
      this._SN = c(2)
    }
//    else
//      throw new Exception("insufficient field for AtgxReadsInfoWrapper construction")
  }

  override def toString: String = {
    s" ${_ID}#${_barcode}#${_SN}}"
  }

  def setID(newID: Long): Unit = {
    _ID = newID
  }

  def getID(): Long = {
    _ID
  }

  def setBarcode(newBarcode: Int)= {
    _barcode = newBarcode
  }

  def getBarcode(): Int = {
    _barcode
  }

  def setSN(newSN: String)= {
    _SN = newSN
  }

  def getSN(): String = {
    _SN
  }
}


