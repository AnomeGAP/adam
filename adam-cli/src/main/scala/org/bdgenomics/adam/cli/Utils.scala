package org.bdgenomics.adam.cli

object Utils {
  def reverseComplementary(s: String): String = {
    val complementary = Map('A' -> 'T', 'T' -> 'A', 'C' -> 'G', 'G' -> 'C')
    var i = 0
    var j = s.length - 1
    val c = s.toCharArray

    while (i < j) {
      val temp = c(i)
      c(i) = complementary(c(j))
      c(j) = complementary(temp)
      i = i + 1
      j = j - 1
    }

    if (s.length % 2 != 0) c(i) = complementary(c(i))

    String.valueOf(c)
  }
}
