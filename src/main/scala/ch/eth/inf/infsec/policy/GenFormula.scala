package ch.eth.inf.infsec.policy

// This is explicitly not a case class, such that each instance represent a fresh variable name.
class VariableID(val nameHint: String, val freeID: Int = -1) extends Serializable {
  def isFree: Boolean = freeID >= 0
  override def toString: String = if (isFree) s"$nameHint@$freeID" else "<bound>"
}

trait VariableMapper[V, W] {
  def bound(variable: V): (W, VariableMapper[V, W])
  def map(variable: V): W
}

class VariableResolver(variables: Map[String, VariableID]) extends VariableMapper[String, VariableID] {
  override def bound(variable: String): (VariableID, VariableResolver) = {
    val id = new VariableID(variable)
    (id, new VariableResolver(variables.updated(variable, id)))
  }

  override def map(variable: String): VariableID = variables(variable)
}

class VariablePrinter(variables: Map[VariableID, String]) extends VariableMapper[VariableID, String] {
  override def bound(variable: VariableID): (String, VariablePrinter) = {
    def exists(name: String): Boolean = variables.values.exists(_ == name)
    val uniqueName = if (exists(variable.nameHint))
        (1 to Int.MaxValue).view.map(i => variable.nameHint + "_" + i.toString).find(!exists(_)).get
      else
        variable.nameHint
    (uniqueName, new VariablePrinter(variables.updated(variable, uniqueName)))
  }

  override def map(variable: VariableID): String = variables(variable)
}

sealed trait Term[V] extends Serializable {
  def freeVariables: Set[V]
  def map[W](mapper: VariableMapper[V, W]): Term[W]
}

case class ConstInteger[V](value: Long) extends Term[V] {
  override val freeVariables: Set[V] = Set.empty
  override def map[W](mapper: VariableMapper[V, W]): ConstInteger[W] = ConstInteger(value)
  override def toString: String = value.toString
}

case class ConstString[V](value: String) extends Term[V] {
  override val freeVariables: Set[V] = Set.empty
  override def map[W](mapper: VariableMapper[V, W]): ConstString[W] = ConstString(value)
  override def toString: String = "\"" + value + "\""
}

case class Var[V](variable: V) extends Term[V] {
  override val freeVariables: Set[V] = Set(variable)
  override def map[W](mapper: VariableMapper[V, W]): Var[W] = Var(mapper.map(variable))
  override def toString: String = variable.toString
}

case class Interval(lower: Int, upper: Option[Int]) {
  def check: List[String] =
    // TODO(JS): Do we want to allow empty intervals?
    if (upper.isDefined && upper.get <= lower) List(s"$this is not a valid interval")
    else if (lower < 0) List(s"interval $this contains negative values")
    else Nil

  override def toString: String = upper match {
    case None => s"[$lower,*)"
    case Some(u) => s"[$lower,$u)"
  }
}

object Interval {
  val any = Interval(0, None)
}

sealed trait GenFormula[V] extends Serializable {
  def atoms: Set[Pred[V]]
  def freeVariables: Set[V]
  def map[W](mapper: VariableMapper[V, W]): GenFormula[W]
  def check: List[String]
}

case class True[V]() extends GenFormula[V] {
  override val atoms: Set[Pred[V]] = Set.empty
  override val freeVariables: Set[V] = Set.empty
  override def map[W](mapper: VariableMapper[V, W]): True[W] = True()
  override def check: List[String] = Nil
  override def toString: String = "TRUE"
}

case class False[V]() extends GenFormula[V] {
  override val atoms: Set[Pred[V]] = Set.empty
  override val freeVariables: Set[V] = Set.empty
  override def map[W](mapper: VariableMapper[V, W]): False[W] = False()
  override def check: List[String] = Nil
  override def toString: String = "FALSE"
}

case class Pred[V](relation: String, args: Term[V]*) extends GenFormula[V] {
  override val atoms: Set[Pred[V]] = Set(this)
  override lazy val freeVariables: Set[V] = args.flatMap(_.freeVariables).toSet
  override def map[W](mapper: VariableMapper[V, W]): Pred[W] = Pred(relation, args.map(_.map(mapper)):_*)
  override def check: List[String] = Nil
  override def toString: String = s"$relation(${args.mkString(", ")})"
}

case class Not[V](arg: GenFormula[V]) extends GenFormula[V] {
  override lazy val atoms: Set[Pred[V]] = arg.atoms
  override lazy val freeVariables: Set[V] = arg.freeVariables
  override def map[W](mapper: VariableMapper[V, W]): Not[W] = Not(arg.map(mapper))
  override def check: List[String] = arg.check
  override def toString: String = s"NOT ($arg)"
}

case class And[V](arg1: GenFormula[V], arg2: GenFormula[V]) extends GenFormula[V] {
  override lazy val atoms: Set[Pred[V]] = arg1.atoms ++ arg2.atoms
  override lazy val freeVariables: Set[V] = arg1.freeVariables ++ arg2.freeVariables
  override def map[W](mapper: VariableMapper[V, W]): And[W] = And(arg1.map(mapper), arg2.map(mapper))
  override def check: List[String] = arg1.check ++ arg2.check
  override def toString: String = s"($arg1) AND ($arg2)"
}

case class Or[V](arg1: GenFormula[V], arg2: GenFormula[V]) extends GenFormula[V] {
  override lazy val atoms: Set[Pred[V]] = arg1.atoms ++ arg2.atoms
  override lazy val freeVariables: Set[V] = arg1.freeVariables ++ arg2.freeVariables
  override def map[W](mapper: VariableMapper[V, W]): Or[W] = Or(arg1.map(mapper), arg2.map(mapper))
  override def check: List[String] = arg1.check ++ arg2.check
  override def toString: String = s"($arg1) OR ($arg2)"
}

case class All[V](variable: V, arg: GenFormula[V]) extends GenFormula[V] {
  override lazy val atoms: Set[Pred[V]] = arg.atoms
  override lazy val freeVariables: Set[V] = arg.freeVariables - variable

  override def map[W](mapper: VariableMapper[V, W]): All[W] = {
    val (newVariable, innerMapper) = mapper.bound(variable)
    All(newVariable, arg.map(innerMapper))
  }

  override def check: List[String] = arg.check
  override def toString: String = s"FORALL $variable. $arg"
}

case class Ex[V](variable: V, arg: GenFormula[V]) extends GenFormula[V] {
  override lazy val atoms: Set[Pred[V]] = arg.atoms
  override lazy val freeVariables: Set[V] = arg.freeVariables - variable

  override def map[W](mapper: VariableMapper[V, W]): Ex[W] = {
    val (newVariable, innerMapper) = mapper.bound(variable)
    Ex(newVariable, arg.map(innerMapper))
  }

  override def check: List[String] = arg.check
  override def toString: String = s"EXISTS $variable. $arg"
}

case class Prev[V](interval: Interval, arg: GenFormula[V]) extends GenFormula[V] {
  override lazy val atoms: Set[Pred[V]] = arg.atoms
  override lazy val freeVariables: Set[V] = arg.freeVariables
  override def map[W](mapper: VariableMapper[V, W]): Prev[W] = Prev(interval, arg.map(mapper))
  override def check: List[String] = interval.check ++ arg.check
  override def toString: String = s"PREVIOUS $interval ($arg)"
}

case class Next[V](interval: Interval, arg: GenFormula[V]) extends GenFormula[V] {
  override lazy val atoms: Set[Pred[V]] = arg.atoms
  override lazy val freeVariables: Set[V] = arg.freeVariables
  override def map[W](mapper: VariableMapper[V, W]): Next[W] = Next(interval, arg.map(mapper))
  override def check: List[String] = interval.check ++ arg.check
  override def toString: String = s"NEXT $interval ($arg)"
}

case class Since[V](interval: Interval, arg1: GenFormula[V], arg2: GenFormula[V]) extends GenFormula[V] {
  override lazy val atoms: Set[Pred[V]] = arg1.atoms ++ arg2.atoms
  override lazy val freeVariables: Set[V] = arg1.freeVariables ++ arg2.freeVariables
  override def map[W](mapper: VariableMapper[V, W]): Since[W] = Since(interval, arg1.map(mapper), arg2.map(mapper))
  override def check: List[String] = arg1.check ++ interval.check ++ arg2.check
  override def toString: String = s"($arg1) SINCE $interval ($arg2)"
}

case class Trigger[V](interval: Interval, arg1: GenFormula[V], arg2: GenFormula[V]) extends GenFormula[V] {
  override lazy val atoms: Set[Pred[V]] = arg1.atoms ++ arg2.atoms
  override lazy val freeVariables: Set[V] = arg1.freeVariables ++ arg2.freeVariables
  override def map[W](mapper: VariableMapper[V, W]): Trigger[W] = Trigger(interval, arg1.map(mapper), arg2.map(mapper))
  override def check: List[String] = arg1.check ++ interval.check ++ arg2.check
  override def toString: String = s"($arg1) TRIGGER $interval ($arg2)"
}

case class Until[V](interval: Interval, arg1: GenFormula[V], arg2: GenFormula[V]) extends GenFormula[V] {
  override lazy val atoms: Set[Pred[V]] = arg1.atoms ++ arg2.atoms
  override lazy val freeVariables: Set[V] = arg1.freeVariables ++ arg2.freeVariables
  override def map[W](mapper: VariableMapper[V, W]): Until[W] = Until(interval, arg1.map(mapper), arg2.map(mapper))
  override def check: List[String] = arg1.check ++ interval.check ++ arg2.check
  override def toString: String = s"($arg1) UNTIL $interval ($arg2)"
}

case class Release[V](interval: Interval, arg1: GenFormula[V], arg2: GenFormula[V]) extends GenFormula[V] {
  override lazy val atoms: Set[Pred[V]] = arg1.atoms ++ arg2.atoms
  override lazy val freeVariables: Set[V] = arg1.freeVariables ++ arg2.freeVariables
  override def map[W](mapper: VariableMapper[V, W]): Release[W] = Release(interval, arg1.map(mapper), arg2.map(mapper))
  override def check: List[String] = arg1.check ++ interval.check ++ arg2.check
  override def toString: String = s"($arg1) RELEASE $interval ($arg2)"
}

object GenFormula {
  def implies[V](arg1: GenFormula[V], arg2: GenFormula[V]): GenFormula[V] = Or(Not(arg1), arg2)
  def equiv[V](arg1: GenFormula[V], arg2: GenFormula[V]): GenFormula[V] = And(implies(arg1, arg2), implies(arg2, arg1))
  def once[V](interval: Interval, arg: GenFormula[V]): GenFormula[V] = Since(interval, True(), arg)
  def historically[V](interval: Interval, arg: GenFormula[V]): GenFormula[V] = Trigger(interval, False(), arg)
  def eventually[V](interval: Interval, arg: GenFormula[V]): GenFormula[V] = Until(interval, True(), arg)
  def always[V](interval: Interval, arg: GenFormula[V]): GenFormula[V] = Release(interval, False(), arg)

  def resolve(phi: GenFormula[String]): GenFormula[VariableID] = {
    val freeVariables: Map[String, VariableID] =
      phi.freeVariables.toSeq.sorted.zipWithIndex.map{ case (n, i) => (n, new VariableID(n, i)) }(collection.breakOut)
    phi.map(new VariableResolver(freeVariables))
  }

  def print(phi: GenFormula[VariableID]): GenFormula[String] = {
    val freeVariables: Map[VariableID, String] = phi.freeVariables.map(v => (v, v.nameHint))(collection.breakOut)
    phi.map(new VariablePrinter(freeVariables))
  }

  def pushNegation[V](phi: GenFormula[V]): GenFormula[V] = {
    def pos(phi: GenFormula[V]): GenFormula[V] = phi match {
      case Not(arg) => neg(arg)
      case And(arg1, arg2) => And(pos(arg1), pos(arg2))
      case Or(arg1, arg2) => Or(pos(arg1), pos(arg2))
      case All(bound, arg) => All(bound, pos(arg))
      case Ex(bound, arg) => Ex(bound, pos(arg))
      case Prev(i, arg) => Prev(i, pos(arg))
      case Next(i, arg) => Next(i, pos(arg))
      case Since(i, arg1, arg2) => Since(i, pos(arg1), pos(arg2))
      case Trigger(i, arg1, arg2) => Trigger(i, pos(arg1), pos(arg2))
      case Until(i, arg1, arg2) => Until(i, pos(arg1), pos(arg2))
      case Release(i, arg1, arg2) => Release(i, pos(arg1), pos(arg2))
      case _ => phi
    }

    def neg(phi: GenFormula[V]): GenFormula[V] = phi match {
      case True() => False()
      case False() => True()
      case Not(arg) => pos(arg)
      case And(arg1, arg2) => Or(neg(arg1), neg(arg2))
      case Or(arg1, arg2) => And(neg(arg1), neg(arg2))
      case All(bound, arg) => Ex(bound, neg(arg))
      case Ex(bound, arg) => All(bound, neg(arg))
      case Prev(i, arg) => Or(Prev(i, neg(arg)), Not(Prev(i, True())))  // TODO(JS): Verify this equivalence
      case Next(i, arg) => Or(Next(i, neg(arg)), Not(Next(i, True())))  // TODO(JS): Verify this equivalence
      case Since(i, arg1, arg2) => Trigger(i, neg(arg1), neg(arg2))
      case Trigger(i, arg1, arg2) => Since(i, neg(arg1), neg(arg2))
      case Until(i, arg1, arg2) => Release(i, neg(arg1), neg(arg2))
      case Release(i, arg1, arg2) => Until(i, neg(arg1), neg(arg2))
      case _ => Not(pos(phi))
    }

    pos(phi)
  }
}
