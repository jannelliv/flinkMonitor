package ch.eth.inf.infsec.policy

import fastparse.WhitespaceApi
import fastparse.noApi._

private object PolicyParsers {

  private object Token {
    import fastparse.all._

    val Comment: P0 = P( "(*" ~/ (!"*)" ~ AnyChar).rep ~/ "*)" )
    val LineComment: P0 = P( "#" ~/ CharsWhile(c => c != '\n' && c != '\r'))
    val Whitespace: P0 = P( NoTrace((CharsWhileIn(" \t\n\r") | Comment | LineComment).rep) )

    val Letter: P0 = P( CharIn('a' to 'z', 'A' to 'Z') )
    val Digit: P0 = P( CharIn('0' to '9') )
    val AnyStringExclQuotes: P0 = P( (Letter | Digit | CharIn("_-/:")).rep )
    val AnyString: P0 = P( (Letter | Digit | CharIn("_-/:\'\"")).rep )

    val Identifier: P[String] = P( ((Letter | Digit | "_") ~ AnyString).! )

    val Integer: P[Long] = P( ("-".? ~ Digit.rep(min = 1)).!.map(_.toLong) )
    // TODO(JS): Should use an exact type for rationals.
    val Rational: P[Any] = Fail
    val QuotedString: P[String] = P(
      ("\'[" ~/ AnyString.! ~/ "]\'") | ("\"[" ~/ AnyString.! ~/ "]\"") |
      ("\'" ~/ AnyStringExclQuotes.! ~/ "\'") | ("\"" ~/ AnyStringExclQuotes.! ~/ "\"")
    )

    val Quantity: P[(Int, String)] = P( Digit.rep.!.map(_.toInt) ~/ Letter.?.! )

    val Eventually: P0 = P( "EVENTUALLY" | "SOMETIMES" )
    val Once: P0 = P( "ONCE" )
    val Always: P0 = P( "ALWAYS" )
    val Historically: P0 = P( "PAST_ALWAYS" | "HISTORICALLY" )
  }

  private val WhitespaceWrapper = WhitespaceApi.Wrapper(Token.Whitespace)
  import WhitespaceWrapper._

  private def timeUnit(unit: String): Int = unit match {
    case "d" => 24 * 60 * 60
    case "h" => 60 * 60
    case "m" => 60
    case "s" | "" => 1
    case _ => throw new RuntimeException(s"unrecognized time unit: $unit")  // TODO(JS): Proper error handling
  }

  private def optionalInterval(spec: Option[Interval]): Interval = spec.getOrElse(Interval.any)

  private def leftAssoc(op: P0, sub: P[Formula], mk: (Formula, Formula) => Formula): P[Formula] =
    P(sub.rep(min = 1, sep = op).map(xs => xs.tail.foldLeft(xs.head)(mk)))

  private def rightAssoc(op: P0, sub: P[Formula], mk: (Formula, Formula) => Formula): P[Formula] =
    P(sub.rep(min = 1, sep = op).map(xs => xs.init.foldRight(xs.last)(mk)))

  private def quantifier(op: P0, sub: P[Formula], mk: (String, Formula) => Formula): P[Formula] =
    P( (op ~/ Token.Identifier.rep(min = 1, sep = ",") ~ "." ~/ sub).map{ case (vars, phi) =>
      vars.init.foldRight(mk(vars.last, phi))(mk)} )

  private def unaryTemporal(op: P0, sub: P[Formula], mk: (Interval, Formula) => Formula): P[Formula] =
    P( (op ~/ TimeInterval.? ~/ sub).map{ case (i, phi) => mk(optionalInterval(i), phi) } )

  // BUG(JS): Does not follow the longest match used by ocamllex. Identifiers such as "5_"
  // are not parsed correctly.
  val Term: P[Term] = P(
    Token.Integer.map(Const) | Token.Rational.map(Const) | Token.QuotedString.map(Const) |
    Token.Identifier.map(Free(-1, _))
  )

  val TimeInterval: P[Interval] = P( (("(" | "[").! ~/ Token.Quantity ~/ "," ~/
    ("*" ~ PassWith(None) | Token.Quantity.map(Some(_))) ~/ (")" | "]").!)
    .map{ case (lk, (lb, lu), rbo, rk) =>
      val leftBound = lb * timeUnit(lu) + (if (lk == "(") 1 else 0)
      val rightBound = rbo.map{ case (rb, ru) => rb * timeUnit(ru) + (if (rk == "]") 1 else 0) }
      Interval(leftBound, rightBound)
    }
  )

  val Formula9: P[Formula] = P(
    ("(" ~/ Formula1 ~/ ")") | "TRUE" ~/ PassWith(Formula.True) | "FALSE" ~/ PassWith(False()) |
    ("NOT" ~/ Formula9).map(Not) |
    (Token.Identifier ~/ "(" ~/ Term.rep(sep = ",") ~ ")").map(x => Pred(x._1, x._2:_*))
  )

  val Formula8: P[Formula] = leftAssoc("AND", Formula9, And)

  val Formula7: P[Formula] = leftAssoc("OR", Formula8, Or)

  val Formula6: P[Formula] = rightAssoc("IMPLIES", Formula7, Formula.Implies)

  val Formula5: P[Formula] = leftAssoc("EQUIV", Formula6, Formula.Equiv)

  val Formula2: P[Formula] = P(
    quantifier("EXISTS", Formula2, Ex) |
    quantifier("FORALL", Formula2, All) |
    unaryTemporal("PREVIOUS", Formula2, Prev) |
    unaryTemporal("NEXT", Formula2, Next) |
    unaryTemporal(Token.Eventually, Formula2, Formula.Eventually) |
    unaryTemporal(Token.Once, Formula2, Formula.Once) |
    unaryTemporal(Token.Always, Formula2, Formula.Always) |
    unaryTemporal(Token.Historically, Formula2, Formula.Historically) |
    Formula5
  )

  val Formula1: P[Formula] = P(
    (Formula2 ~/ ( StringIn("SINCE", "UNTIL").! ~/ TimeInterval.? ~/ Formula2).rep)
      .map{ case (phi1, ops) =>
        ops.foldRight(identity[Formula](_)){ case ((op, i, phi), mkr) => lhs =>
          val rhs = mkr(phi)
          val interval = optionalInterval(i)
          op match {
            case "SINCE" => Since(interval, lhs, rhs)
            case "UNTIL" => Until(interval, lhs, rhs)
          }
        }(phi1)
      }
  )

  val Policy: P[Formula] = P( Token.Whitespace ~ Formula1 ~ Token.Whitespace ~ End )
}

object Policy {
  def parse(input: String): Either[String, Formula] = PolicyParsers.Policy.parse(input) match {
    case Parsed.Success(formula, _) => Right(formula)
    case Parsed.Failure(_, index, extra) =>
      val input = extra.input
      Left(s"syntax error near ${input.repr.prettyIndex(input, index)}")
  }

  def read(input: String): Either[String, Formula] =
    parse(input).right.flatMap(_.check(Context.empty()))
}
