package ch.eth.inf.infsec

class SeqStream[T](val seq: Seq[T]) extends Stream[T] {
  override type Self[A] = SeqStream[A]

  def map[U:TypeInfo](f: T => U) = new Self(seq.map(f))
  override def flatMap[U:TypeInfo](f: T => TraversableOnce[U]) = new Self(seq.flatMap(f))

  override def filter(f: T => Boolean): SeqStream[T] = new Self(seq.filter(f))

  override def print: SeqStream[T] = {seq.foreach(println);this}
}
