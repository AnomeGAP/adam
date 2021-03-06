package org.bdgenomics.adam.cli

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
   * _SN: sample name information, used in a scenario that reads from multiple samples are pooled together
   *      in seqGraph downstream pipelines.
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
  }

  override def toString: String = {
    s" ${"%010d".format(_ID)}#${"%010d".format(_barcode)}#${_SN}"
  }

  def setID(newID: Long): Unit = {
    _ID = newID
  }

  def getID: Long = {
    _ID
  }

  def setBarcode(newBarcode: Int): Unit = {
    _barcode = newBarcode
  }

  def getBarcode: Int = {
    _barcode
  }

  def setSN(newSN: String): Unit = {
    _SN = newSN
  }

  def getSN: String = {
    _SN
  }
}

