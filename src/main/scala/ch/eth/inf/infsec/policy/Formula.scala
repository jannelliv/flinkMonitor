package ch.eth.inf.infsec.policy

import scala.collection.mutable.ArrayBuffer

sealed trait Term {
  def freeVariables: Set[Free]
  def check(implicit ctxt: Context): Term
}

case class Const(value: Any) extends Term {
  override def freeVariables: Set[Free] = Set.empty
  override def check(implicit ctxt: Context): Const = this
  override def toString = s"'$value'"
}

case class Free(index: Int, name: String) extends Term {
  override def freeVariables: Set[Free] = Set(this)
  override def check(implicit ctxt: Context): Term = ctxt.variable(name)
  override def toString = s"$name[$index]"
}

case class Bound(index: Int, name: String) extends Term {
  override def freeVariables: Set[Free] = Set.empty
  override def check(implicit ctxt: Context): Term = ctxt.variable(name)
  override def toString = s"$name[b$index]"
}

case class Context(frees: ArrayBuffer[String], bounds: List[String] = Nil) {
  def withBound(name: String): Context = Context(frees, name :: bounds)

  def variable(name: String): Term = {
    val b = bounds.indexOf(name)
    if (b >= 0)
      return Bound(b, name)

    val f = frees.indexOf(name)
    if (f >= 0)
      Free(f, name)
    else {
      frees += name
      Free(frees.length - 1, name)
    }
  }
}

object Context {
  def empty(): Context = new Context(new ArrayBuffer[String]())
}

case class Interval(lower: Int, upper: Option[Int]) {
  def check: Either[String, Interval] =
    if (upper.isDefined && upper.get < lower) Left(s"$this is not a valid interval")
    else if (lower < 0) Left(s"interval $this contains negative values")
    else Right(this)

  override def toString: String = upper match {
    case None => s"[$lower,*)"
    case Some(u) => s"[$lower,$u)"
  }
}

object Interval {
  val any = Interval(0, None)
}

sealed trait Formula {
  def atoms: Set[Pred]
  def check(implicit ctxt: Context): Either[String, Formula]

  lazy val freeVariables: Set[Free] = atoms.flatMap(pred => pred.freeVariables)
}

case class False() extends Formula {
  override val atoms: Set[Pred] = Set.empty
  override def check(implicit ctxt: Context) = Right(this)
  override def toString = "FALSE"
}

case class Pred(relation: String, args: Term*) extends Formula {
  override val atoms: Set[Pred] = Set(this)
  override lazy val freeVariables: Set[Free] = args.flatMap(t => t.freeVariables).toSet
  override def check(implicit ctxt: Context) = Right(Pred(relation, args.map(t => t.check):_*))
  override def toString = s"$relation(${args.map(x => x.toString).mkString(", ")})"
}

case class Not(arg: Formula) extends Formula {
  override lazy val atoms: Set[Pred] = arg.atoms
  override def check(implicit ctxt: Context): Either[String, Not] = arg.check.right.map(Not)
  override def toString: String = s"NOT ($arg)"
}

case class And(arg1: Formula, arg2: Formula) extends Formula {
  override lazy val atoms: Set[Pred] = arg1.atoms.union(arg2.atoms)
  override def check(implicit ctxt: Context): Either[String, And] =
    arg1.check.right.flatMap(a1 => arg2.check.right.map(a2 => And(a1, a2)))  // applicative, anyone?
  override def toString: String = s"($arg1) AND ($arg2)"
}

case class Or(arg1: Formula, arg2: Formula) extends Formula {
  override lazy val atoms: Set[Pred] = arg1.atoms.union(arg2.atoms)
  override def check(implicit ctxt: Context): Either[String, Or] =
    arg1.check.right.flatMap(a1 => arg2.check.right.map(a2 => Or(a1, a2)))
  override def toString: String = s"($arg1) OR ($arg2)"
}

case class All(bound: String, arg: Formula) extends Formula {
  override lazy val atoms: Set[Pred] = arg.atoms
  override def check(implicit ctxt: Context): Either[String, All] = {
    val innerCtxt = ctxt.withBound(bound)
    arg.check(innerCtxt).right.map(a => All(bound, a))
  }
  override def toString: String = s"ALL $bound. $arg"
}

case class Ex(bound: String, arg: Formula) extends Formula {
  override lazy val atoms: Set[Pred] = arg.atoms
  override def check(implicit ctxt: Context): Either[String, Ex] = {
    val innerCtxt = ctxt.withBound(bound)
    arg.check(innerCtxt).right.map(a => Ex(bound, a))
  }
  override def toString: String = s"EX $bound. $arg"
}

case class Prev(interval: Interval, arg: Formula) extends Formula {
  override lazy val atoms: Set[Pred] = arg.atoms
  override def check(implicit ctxt: Context): Either[String, Prev] =
    interval.check.right.flatMap(i => arg.check.right.map(a => Prev(i, a)))
  override def toString: String = s"PREV $interval ($arg)"
}

case class Next(interval: Interval, arg: Formula) extends Formula {
  override lazy val atoms: Set[Pred] = arg.atoms
  override def check(implicit ctxt: Context): Either[String, Next] =
    interval.check.right.flatMap(i => arg.check.right.map(a => Next(i, a)))
  override def toString: String = s"NEXT $interval ($arg)"
}

case class Once(interval: Interval, arg: Formula) extends Formula {
  override lazy val atoms: Set[Pred] = arg.atoms
  override def check(implicit ctxt: Context): Either[String, Once] =
    interval.check.right.flatMap(i => arg.check.right.map(a => Once(i, a)))
  override def toString: String = s"ONCE $interval ($arg)"
}

case class Eventually(interval: Interval, arg: Formula) extends Formula {
  override lazy val atoms: Set[Pred] = arg.atoms
  override def check(implicit ctxt: Context): Either[String, Eventually] =
    interval.check.right.flatMap(i => arg.check.right.map(a => Eventually(i, a)))
  override def toString: String = s"EVENTUALLY $interval ($arg)"
}

case class Historically(interval: Interval, arg: Formula) extends Formula {
  override lazy val atoms: Set[Pred] = arg.atoms
  override def check(implicit ctxt: Context): Either[String, Historically] =
    interval.check.right.flatMap(i => arg.check.right.map(a => Historically(i, a)))
  override def toString: String = s"HISTORICALLY $interval ($arg)"
}

case class Always(interval: Interval, arg: Formula) extends Formula {
  override lazy val atoms: Set[Pred] = arg.atoms
  override def check(implicit ctxt: Context): Either[String, Always] =
    interval.check.right.flatMap(i => arg.check.right.map(a => Always(i, a)))
  override def toString: String = s"ALWAYS $interval ($arg)"
}

case class Since(interval: Interval, arg1: Formula, arg2: Formula) extends Formula {
  override lazy val atoms: Set[Pred] = arg1.atoms.union(arg2.atoms)
  override def check(implicit ctxt: Context): Either[String, Since] =
    interval.check.right.flatMap(i => arg1.check.right.flatMap(a1 =>
      arg2.check.right.map(a2 => Since(i, a1, a2))))
  override def toString: String = s"($arg1) SINCE $interval ($arg2)"
}

case class Until(interval: Interval, arg1: Formula, arg2: Formula) extends Formula {
  override lazy val atoms: Set[Pred] = arg1.atoms.union(arg2.atoms)
  override def check(implicit ctxt: Context): Either[String, Until] =
    interval.check.right.flatMap(i => arg1.check.right.flatMap(a1 =>
      arg2.check.right.map(a2 => Until(i, a1, a2))))
  override def toString: String = s"($arg1) UNTIL $interval ($arg2)"
}

object Formula {
  val True = Not(False())

  def Implies(arg1: Formula, arg2: Formula): Formula = Or(Not(arg1), arg2)

  def Equiv(arg1: Formula, arg2: Formula): Formula = And(Implies(arg1, arg2), Implies(arg2, arg1))
}
