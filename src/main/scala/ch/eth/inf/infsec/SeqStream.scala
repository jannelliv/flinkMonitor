package ch.eth.inf.infsec

class SeqStream[T](val seq: Seq[T]) extends Stream[T] {
  override type Self[A] = SeqStream[A]

  override def map[U](f: T => U) = new Self(seq.map(f))
  override def flatMap[U](f: T => TraversableOnce[U]) = new Self(seq.flatMap(f))
}
