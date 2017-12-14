package ch.eth.inf.infsec

import scala.language.higherKinds

trait TypeInfo[A]{
}
object TypeInfo{
  def apply[A](): TypeInfo[A] = new TypeInfo[A](){}
}

trait Stream[T] {
  type Self[A] <: Stream[A]

  def map[U:TypeInfo](f: T => U): Self[U]
  def flatMap[U:TypeInfo](f: T => TraversableOnce[U]): Self[U]
  def filter(fun: T => Boolean): Self[T]
  def print:Self[T]
}
