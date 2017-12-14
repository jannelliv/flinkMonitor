package ch.eth.inf.infsec
package slicer

import ch.eth.inf.infsec.policy.Formula

trait Slicer {
  val formula: Formula
  val degree: Int

  //TODO(SK): to make the consistant abstract interface we need to pass TypeInfo around. Not sure if there is a better way
  def apply(source: Stream[Event])(implicit in:TypeInfo[Event], out:TypeInfo[(Int, Event)]): source.Self[(Int, Event)]
}
