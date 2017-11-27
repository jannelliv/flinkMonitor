package ch.eth.inf.infsec

import scala.language.higherKinds

trait DataStream[T] {
  type Self[A] <: DataStream[A]

  def flatMap[U](f: T => TraversableOnce[U]): Self[U]
}
