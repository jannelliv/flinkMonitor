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
    val AnyString: P0 = P( (Letter | Digit | CharIn("_-/:\\'\"")).rep )

    val Identifier: P[String] = P( ((Letter | Digit | "_") ~ AnyString).! )

    val Integer: P[Long] = P( ("-".? ~ Digit.rep(min = 1)).!.map(_.toLong) )
    // TODO(JS): Should use an exact type for rationals.
    val Rational: P[Any] = Fail
    val QuotedString: P[String] = P(
      ("\'[" ~/ AnyString.! ~/ "]\'") | ("\"[" ~/ AnyString.! ~/ "]\"") |
      ("\'" ~/ AnyString.! ~/ "\'") | ("\"" ~/ AnyString.! ~/ "\"")
    )
  }

  private val WhitespaceWrapper = WhitespaceApi.Wrapper(Token.Whitespace)
  import WhitespaceWrapper._

  val Term: P[Term] = P(NoCut(
    Token.Integer.map(Const) | Token.Rational.map(Const) | Token.QuotedString.map(Const) |
    Token.Identifier.map(Free(-1, _))
  ))

  private def quantifier(name: String, mk: (String, Formula) => Formula): P[Formula] =
    P((name ~ Token.Identifier.rep(min = 1, sep = ",") ~ "." ~ Formula0).map(x =>
      x._1.tail.foldRight(mk(x._1.head, x._2))(mk))).opaque(name)

  private def leftAssoc(op: P0, sub: P[Formula], mk: (Formula, Formula) => Formula): P[Formula] =
    P(sub.rep(min = 2, sep = op).map(xs => xs.tail.foldLeft(xs.head)(mk)))

  val Formula9: P[Formula] = P(
    ("(" ~ Formula0 ~ ")") | "TRUE" ~ PassWith(Not(False())) | "FALSE" ~ PassWith(False()) |
    ("NOT" ~ Formula9).map(Not) |
    (Token.Identifier ~ "(" ~ Term.rep(sep = ",") ~ ")").map(x => Pred(x._1, x._2:_*))
  )

  val Formula1: P[Formula] = P(
    leftAssoc("AND", Formula9, And) | leftAssoc("OR", Formula9, Or) | Formula9
  )

  val Formula0: P[Formula] = P(
    quantifier("EX", Ex) | quantifier("FA", All) | Formula1
  )

  // TODO(JS): Parse the remaining operators

  val Policy: P[Formula] = P( Token.Whitespace ~ Formula0 ~ Token.Whitespace ~ End )
}

object Policy {
  def parse(input: String): Either[String, Formula] = PolicyParsers.Policy.parse(input) match {
    case Parsed.Success(formula, _) => Right(formula)
    case Parsed.Failure(_, index, extra) =>
      val input = extra.input
      Left(s"Syntax error at ${input.repr.prettyIndex(input, index)}")
  }

  def read(input: String): Either[String, Formula] =
    parse(input).right.flatMap(_.check(Context.empty()))
}
