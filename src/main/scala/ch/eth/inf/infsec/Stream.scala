package ch.eth.inf.infsec

import scala.language.higherKinds

trait Stream[T] {
  type Self[A] <: Stream[A]

  def map[U](f: T => U): Self[U]
  def flatMap[U](f: T => TraversableOnce[U]): Self[U]

}
