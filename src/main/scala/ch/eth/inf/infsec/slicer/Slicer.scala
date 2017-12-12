package ch.eth.inf.infsec
package slicer

import ch.eth.inf.infsec.policy.Formula

trait Slicer {
  val formula: Formula
  val degree: Int

  //def apply(source: Stream[Event]): source.Self[(Int, Event)]
  def apply(event: Event): TraversableOnce[(Int, Event)]
}
