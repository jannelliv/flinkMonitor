package ch.ethz.infsec.policy

import fastparse.WhitespaceApi
import fastparse.noApi._
import ch.ethz.infsec.monitor._


private object PolicyParsers {

  private type FormulaS = GenFormula[String]

  private object Token {
    import fastparse.all._

    val Comment: P0 = P( "(*" ~/ (!"*)" ~ AnyChar).rep ~/ "*)" )
    val LineComment: P0 = P( "#" ~/ CharsWhile(c => c != '\n' && c != '\r'))
    val Whitespace: P0 = P( NoTrace((CharsWhileIn(" \t\n\r") | Comment | LineComment).rep) )

    val Letter: P0 = P( CharIn('a' to 'z', 'A' to 'Z') )
    val Digit: P0 = P( CharIn('0' to '9') )
    val AnyStringExclQuotes: P0 = P( (Letter | Digit | CharIn("_-/:")).rep )
    val NonDigit: P0 = P( Letter | CharIn("_-/:\'\"") )
    val AnyString: P0 = P( (NonDigit | Digit).rep )

    val Identifier: P[String] = P( ((Letter | Digit | "_") ~ AnyString).! )

    val Integer: P[Long] = P( ("-".? ~ Digit.rep(min = 1) ~ !NonDigit).!.map(_.toLong) )
    // TODO(JS): Should use an exact type for rationals.
    val Rational: P[Any] = Fail
    val QuotedString: P[String] = P(
      ("\'[" ~/ AnyString.! ~/ "]\'") | ("\"[" ~/ AnyString.! ~/ "]\"") |
      ("\'" ~/ AnyStringExclQuotes.! ~/ "\'") | ("\"" ~/ AnyStringExclQuotes.! ~/ "\"")
    )

    val Quantity: P[(Int, String)] = P( Digit.rep.!.map(_.toInt) ~/ Letter.?.! )

    def keyword(word: String): P0 = P( word ~ !(NonDigit | Digit) )(sourcecode.Name(s"`$word`"))

    val Eventually: P0 = P( keyword("EVENTUALLY") | keyword("SOMETIMES") )
    val Once: P0 = P( keyword("ONCE") )
    val Always: P0 = P( keyword("ALWAYS") )
    val Historically: P0 = P( keyword("PAST_ALWAYS") | keyword("HISTORICALLY") )
    val SinceUntil: P0 = P( keyword("SINCE") | keyword("UNTIL") )
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

  private def leftAssoc(op: P0, sub: P[FormulaS], subp: P[FormulaS], mk: (FormulaS, FormulaS) => FormulaS): P[FormulaS] =
    (sub.rep(min = 1, sep = op) ~ (op ~ subp).?).map(xs => {
      val xs2 = xs._1 ++ xs._2
      xs2.tail.foldLeft(xs2.head)(mk)
    })

  private def rightAssoc(op: P0, sub: P[FormulaS], subp: P[FormulaS], mk: (FormulaS, FormulaS) => FormulaS): P[FormulaS] =
    (sub.rep(min = 1, sep = op) ~ (op ~ subp).?).map(xs => {
      val xs2 = xs._1 ++ xs._2
      xs2.init.foldRight(xs2.last)(mk)
    })

  private def quantifier(op: P0, sub: P[FormulaS], mk: (String, FormulaS) => FormulaS): P[FormulaS] =
    (op ~/ Token.Identifier.rep(min = 1, sep = ",") ~ "." ~/ sub).map { case (vars, phi) =>
      vars.init.foldRight(mk(vars.last, phi))(mk)
    }

  private def unaryTemporal(op: P0, sub: P[FormulaS], mk: (Interval, FormulaS) => FormulaS): P[FormulaS] =
    (op ~/ TimeInterval.? ~/ sub).map{ case (i, phi) => mk(optionalInterval(i), phi) }

  // TODO(JS): Support decimal numbers
  val Term: P[Term[String]] = P(
    Token.Integer.map(Const[String](_)) /*| Token.Rational.map(Const)*/ |
    Token.QuotedString.map(Const[String](_)) | Token.Identifier.map(Var(_))
  )

  val TimeInterval: P[Interval] = P( (("(" | "[").! ~/ Token.Quantity ~/ "," ~/
    ("*" ~ PassWith(None) | Token.Quantity.map(Some(_))) ~/ (")" | "]").!)
    .map{ case (lk, (lb, lu), rbo, rk) =>
      val leftBound = lb * timeUnit(lu) + (if (lk == "(") 1 else 0)
      val rightBound = rbo.map{ case (rb, ru) => rb * timeUnit(ru) + (if (rk == "]") 1 else 0) }
      Interval(leftBound, rightBound)
    }
  )

  val Formula9: P[FormulaS] = P(
    ("(" ~/ Formula1 ~/ ")") |
    Token.keyword("TRUE") ~/ PassWith(True[String]()) | Token.keyword("FALSE") ~/ PassWith(False[String]()) |
    (Token.keyword("NOT") ~/ Formula9).map(Not(_)) |
    (Token.Identifier ~ "(" ~/ Term.rep(sep = ",") ~ ")").map(x => Pred(x._1, x._2:_*)) |
    (Term ~ "=" ~/ Term).map(x => Pred("__eq", x._1, x._2)) |
    (Term ~ "<" ~/ Term).map(x => Pred("__less", x._1, x._2))
  )

  val Formula8: P[FormulaS] = P( leftAssoc(Token.keyword("AND"), Formula9, Formula2Prefix, And(_, _)) )

  val Formula7: P[FormulaS] = P( leftAssoc(Token.keyword("OR"), Formula8, Formula2Prefix, Or(_, _)) )

  val Formula6: P[FormulaS] = P( rightAssoc(Token.keyword("IMPLIES"), Formula7, Formula2Prefix, GenFormula.implies) )

  val Formula5: P[FormulaS] = P( leftAssoc(Token.keyword("EQUIV"), Formula6, Formula2Prefix, GenFormula.equiv) )

  val Formula2Prefix: P[FormulaS] = P(
    unaryTemporal(Token.keyword("PREVIOUS"), Formula2, Prev(_, _)) |
    unaryTemporal(Token.keyword("NEXT"), Formula2, Next(_, _)) |
    unaryTemporal(Token.Eventually, Formula2, GenFormula.eventually) |
    unaryTemporal(Token.Once, Formula2, GenFormula.once) |
    unaryTemporal(Token.Always, Formula2, GenFormula.always) |
    unaryTemporal(Token.Historically, Formula2, GenFormula.historically) |
    quantifier(Token.keyword("EXISTS"), Formula2, Ex(_, _)) |
    quantifier(Token.keyword("FORALL"), Formula2, All(_, _))
  )

  val Formula2: P[FormulaS] = P( Formula2Prefix | Formula5 )

  val Formula1: P[FormulaS] = P(
    (Formula2 ~ ( Token.SinceUntil.! ~/ TimeInterval.? ~/ Formula2).rep)
      .map{ case (phi1, ops) =>
        ops.foldRight(identity[FormulaS](_)){ case ((op, i, phi), mkr) => lhs =>
          val rhs = mkr(phi)
          val interval = optionalInterval(i)
          op match {
            case "SINCE" => Since(interval, lhs, rhs)
            case "UNTIL" => Until(interval, lhs, rhs)
          }
        }(phi1)
      }
  )

  val Policy: P[FormulaS] = P( Token.Whitespace ~ Formula1 ~ Token.Whitespace ~ End )
}

object Policy {
  def parse(input: String): Either[String, GenFormula[String]] = PolicyParsers.Policy.parse(input) match {
    case Parsed.Success(formula, _) => Right(formula)
    case Parsed.Failure(_, index, extra) =>
      val input = extra.input
      Left(s"syntax error near ${input.repr.prettyIndex(input, index)}")
  }

  def read(input: String): Either[String, Formula] =
    parse(input).right.flatMap { rawFormula =>
      rawFormula.check match {
        case Nil => Right(GenFormula.resolve(rawFormula))
        case error1 :: _ => Left(error1)
      }
    }
}
