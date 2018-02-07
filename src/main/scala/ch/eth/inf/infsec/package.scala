package ch.eth.inf

import scala.collection.generic.CanBuildFrom

package object infsec {
  trait CloseableIterable[T] extends Iterable[T] {
    def close(): Unit
  }

  implicit class SequentiallyGroupBy[+A](val it: Iterator[A]) {
    def sequentiallyGroupBy[K, That](key: A => K)(implicit cbf: CanBuildFrom[Seq[A], A, That]): Iterator[(K, That)] =
      new Iterator[(K, That)] {
        var buffer: Option[A] = None

        override def hasNext: Boolean = buffer.isDefined || it.hasNext

        override def next(): (K, That) = {
          var currentItem = buffer.getOrElse {
            it.next()
          }
          buffer = None
          val currentKey = key(currentItem)
          val builder = cbf()
          builder += currentItem

          while (it.hasNext && buffer.isEmpty) {
            currentItem = it.next()
            if (key(currentItem) == currentKey)
              builder += currentItem
            else
              buffer = Some(currentItem)
          }
          (currentKey, builder.result())
        }
      }
  }
}
