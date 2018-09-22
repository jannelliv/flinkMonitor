package ch.eth.inf.infsec

import ch.eth.inf.infsec.trace.{Domain, IntegralValue, StringValue}

package object slicer {
  class SlicerParser extends Serializable {
    def parseDomain(str: String): Domain = {
      val value: Domain = if (str.startsWith("\""))
        StringValue(str.stripPrefix("\"").stripSuffix("\""))
      else
        IntegralValue(str.toLong)
      value
    }

    private def parseHeavy(str: String): IndexedSeq[(Int, Set[Domain])] = {
      val it = str.substring(1, str.length - 1).split("\\),\\(")
      it.map(str => {
        val tuple = str.split(",\\(")
        if (tuple(1).length > 1) {
          val domain = tuple(1).substring(0, tuple(1).length - 1).split(",").map(parseDomain)
          (Integer.parseInt(tuple(0)), domain.toSet)
        } else
          (Integer.parseInt(tuple(0)), Set.empty[Domain])
      })
    }

    private def parseNestedIt(str: String): IndexedSeq[IndexedSeq[Int]] = {
      val it = str.substring(1, str.length - 1).split("\\),\\(")

      it.toIndexedSeq.map(_.split(",")).map(a => a.map(Integer.parseInt).toIndexedSeq)
    }

    def getParallelism(str: String): Int = {
      val trim = str.substring(2, str.length - 2)

      val params = trim.split("\\},\\{")

      val simpleShares = parseNestedIt(params(1))(0)
      if (simpleShares.isEmpty) 1 else simpleShares.product
    }

    def parseSlicer(str: String): (IndexedSeq[(Int, Set[Domain])], IndexedSeq[IndexedSeq[Int]], Array[Array[Int]]) ={
      val trim = str.substring(2, str.length - 2)

      val params = trim.split("\\},\\{")

      val heavy = parseHeavy(params(0))
      val shares = parseNestedIt(params(1))
      val seeds = parseNestedIt(params(2))

      (heavy, shares, seeds.map(_.toArray).toArray)
    }

    private def stringifyDomain(domain: Set[Domain]): String = {
      val it = domain.iterator
      val sb = new StringBuilder
      while (it.hasNext) {
        it.next() match {
          case StringValue(s) =>  sb ++= "\"%s\"".format(s)
          case IntegralValue(i) => sb ++= "%d".format(i)
        }
        if (it.hasNext) sb ++= ","
      }
      sb.mkString
    }

    private def stringifyHeavy(heavy: Iterable[(Int, Set[Domain])]): String = {
      val it = heavy.iterator
      val sb = new StringBuilder

      while (it.hasNext) {
        val h = it.next
        sb ++= "(%d,(%s))".format(h._1, stringifyDomain(h._2))
        if (it.hasNext) sb ++= ","
      }
      "{%s}".format(sb.mkString)
    }

    private def stringifyNestedIt(input: Iterable[Iterable[Int]]): String = {
      val it = input.iterator
      val sb = new StringBuilder
      while (it.hasNext) {
        sb ++= "(%s)".format(it.next.mkString(","))
        if (it.hasNext) sb ++= ","
      }
      "{%s}".format(sb.mkString)
    }

    def stringify(heavy: Iterable[(Int, Set[Domain])], shares: Iterable[Iterable[Int]], seeds: Array[Array[Int]]): String = {
      val itSeeds = seeds.toIterable.map(_.toIterable)

      val sb = new StringBuilder

      sb ++= stringifyHeavy(heavy) + ","
      sb ++= stringifyNestedIt(shares) + ","
      sb ++= stringifyNestedIt(itSeeds)

      "{%s}".format(sb.mkString)
    }
  }
}
