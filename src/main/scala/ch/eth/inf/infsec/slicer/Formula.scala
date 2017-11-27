package ch.eth.inf.infsec.slicer

// TODO(JS): Move to different package, these are not specific to slicing (at least once proper formulas can be encoded).

sealed trait Term

case class Const(value: Any) extends Term {
  override def toString = s"'$value'"
}
case class FreeVar(index: Int) extends Term {
  require(index >= 0)
  override def toString = s"x$index"
}
case class BoundVar(index: Int) extends Term {
  require(index >= 0)
  override def toString = s"b$index"
}

case class Atom(relation: String, args: Term*) {
  override def toString = s"$relation(${args.map(x => x.toString).mkString(", ")})"
}

case class Formula(atoms: Set[Atom]) {
  val freeVariables: Int = atoms.map(a =>
      a.args.collect{case FreeVar(i) => i}.reduceOption(_ max _).getOrElse(-1))
    .reduceOption(_ max _).getOrElse(-1) + 1

  override def toString: String = atoms.mkString("{", ", ", "}")
}

