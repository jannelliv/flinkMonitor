package ch.ethz.infsec.slicer

trait Statistics {
  def relationSize(relation: String): Double

  def heavyHitters(relation: String, attribute: Int): Set[Any]
}

object Statistics {
  val constant: Statistics = new Statistics {
    override def relationSize(relation: String): Double = 100.0

    override def heavyHitters(relation: String, attribute: Int): Set[Any] = Set.empty
  }

  def simple(sizes: (String, Double)*): Statistics = new Statistics {
    val map: Map[String, Double] = Map(sizes:_*)

    override def relationSize(relation: String): Double = map(relation)

    override def heavyHitters(relation: String, attribute: Int): Set[Any] = Set.empty
  }
}
