package ch.eth.inf

package object infsec {
  trait CloseableIterable[T] extends Iterable[T] {
    def close(): Unit
  }
}
