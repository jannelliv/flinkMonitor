package ch.eth.inf.infsec

class SeqStream[T](val seq: Seq[T]) extends DataStream[T] {
  override type Self[A] = SeqStream[A]

  override def flatMap[U](f: T => TraversableOnce[U]) = new Self(seq.flatMap(f))
}
