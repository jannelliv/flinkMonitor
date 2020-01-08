package ch.ethz.infsec

package object slicer {
  class SlicerParser extends Serializable {
    def parseDomain(str: String): Any = {
      val value: Any = if (str.startsWith("\""))
        str.stripPrefix("\"").stripSuffix("\"")
      else
        Long.box(str.toLong)
      value
    }

    private def parseHeavy(str: String): IndexedSeq[(Int, Set[Any])] = {
      val it = str.substring(1, str.length - 1).split("\\),\\(")
      it.map(str => {
        val tuple = str.split(",\\(")
        if (tuple(1).length > 1) {
          val domain = tuple(1).substring(0, tuple(1).length - 1).split(",").map(parseDomain)
          (Integer.parseInt(tuple(0)), domain.toSet)
        } else
          (Integer.parseInt(tuple(0)), Set.empty[Any])
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

    def parseSlicer(str: String): (IndexedSeq[(Int, Set[Any])], IndexedSeq[IndexedSeq[Int]], Array[Array[Int]], Int) ={
      val trim = str.substring(2, str.length - 2)

      val params = trim.split("\\},\\{")

      val heavy = parseHeavy(params(0))
      val shares = parseNestedIt(params(1))
      val seeds = parseNestedIt(params(2))
      val maxDegree = parseDomain(params(3))

      (heavy, shares, seeds.map(_.toArray).toArray, maxDegree.toString.toInt)
    }

    private def stringifyDomain(value: Any): String = value match {
      case x: java.lang.Long => x.toString
      case x: java.lang.Integer => x.toString
      case x: String => "\"" + x + "\""
    }

    private def stringifyDomainSet(domain: Set[Any]): String = {
      val it = domain.iterator
      val sb = new StringBuilder
      while (it.hasNext) {
        sb ++= stringifyDomain(it.next())
        if (it.hasNext) sb ++= ","
      }
      sb.mkString
    }

    private def stringifyHeavy(heavy: Iterable[(Int, Set[Any])]): String = {
      val it = heavy.iterator
      val sb = new StringBuilder

      while (it.hasNext) {
        val h = it.next
        sb ++= "(%d,(%s))".format(h._1, stringifyDomainSet(h._2))
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

    def stringify(heavy: Iterable[(Int, Set[Any])], shares: Iterable[Iterable[Int]], seeds: Array[Array[Int]], maxDegree: Int): String = {
      val itSeeds = seeds.toIterable.map(_.toIterable)

      val sb = new StringBuilder

      sb ++= stringifyHeavy(heavy) + ","
      sb ++= stringifyNestedIt(shares) + ","
      sb ++= stringifyNestedIt(itSeeds) + ","
      sb ++= stringifyDomain(maxDegree)

      "{%s}".format(sb.mkString)
    }
  }
}
