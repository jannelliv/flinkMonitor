package ch.eth.inf.infsec.autobalancer

import java.io.Serializable

import ch.eth.inf.infsec.slicer.Statistics
import ch.eth.inf.infsec.trace.{Domain, Record, Timestamp}


object Helpers
{
    def fuse(a:scala.collection.mutable.Map[(Int,Domain),Int],b:scala.collection.mutable.Map[(Int,Domain),Int]):scala.collection.mutable.Map[(Int,Domain),Int] = {
      a ++ b.map { case (k,v) => k -> (v + a.getOrElse(k,0))}
    }
    def heavyHitterFreq(valuePerPosFreq:Double) : Boolean = {
      valuePerPosFreq > 0.2
    }
}
class WindowStatistics extends Statistics with Serializable{
  var lastFrame = 0
  val timestampDeltaBetweenFrames = 0.033; //Todo: turn into config option
  val maxFrames = 150; //Todo: turn into config option
  var frames = Array.fill[FrameStatistic](maxFrames)(new FrameStatistic())
  var relations = scala.collection.mutable.Map[String,Int](); //todo: make param
  var heavyHitter = scala.collection.mutable.Map[(String,Int),Set[Domain]]()
  def recomputeFrameInfo() : Unit = {
    //we can optimize here:
    //   - instead of recomputing relations completely, we can do the delta. namely we subtract frames(lastFrame) from relations and add frames(lastFrame-1) to relations
    //   - for heavy hitters we can do two opts:
    //   - first: we take all the ones we have already and multiply with the old relations (to get occurances again instead of freq)
    //            then we subtract and add like we did with relations
    //    ... this gets complicated, postpone until needed
    //recomputes relation sizes
    relations = relations.map(rel => (rel._1,frames.foldRight(0)((x,acc) => acc + x.relationSize(rel._1))));
    val temp = relations
      .map(rel => (rel._1,frames
        .foldRight(scala.collection.mutable.Map[(Int,Domain),Int]())((x,acc) => Helpers.fuse(acc,x.getRelInfo(rel._1)))
        .map(elm => (elm._1,elm._2 / rel._2))
        .filter(x => Helpers.heavyHitterFreq(x._2))))
    heavyHitter = scala.collection.mutable.Map[(String,Int),Set[Domain]]()
    temp.foreach(x => {
      x._2.foreach( y => {
        heavyHitter(x._1, y._1._1) += y._1._2
      })
    })
  }
  def nextFrame() : Unit = {
    lastFrame += 1
    if(lastFrame > maxFrames)
      lastFrame = 0
    recomputeFrameInfo()
    frames(lastFrame).clearFrame()
  }
  override def relationSize(relation: String): Double = {
    relations(relation)
  }

  override def heavyHitters(relation: String, attribute: Int): Set[Domain] = {
    heavyHitter(relation,attribute)
  }

  //todo: the timestamp is atm nonsense, needs fixing
  var frameTimestamp:Double = 0//new trace.Timestamp();
  var first = true;
  def addEvent(event : Record) : Unit = {
    if(first)
      frameTimestamp = event.timestamp
    frames(lastFrame).addEvent(event)
    while(event.timestamp > frameTimestamp + timestampDeltaBetweenFrames) {
      nextFrame()
      frameTimestamp = frameTimestamp + timestampDeltaBetweenFrames
    }
  }
}
class FrameStatistic extends Serializable
{
  var relations = scala.collection.mutable.Map[String,Int]();
  // (Name, Attribute, Value) -> Frequency
  var valueOccurances = scala.collection.mutable.Map[String,scala.collection.mutable.Map[(Int,Domain),Int]]();
  //      var isProcessed = false; - todo: process opt, so that we store count first, then transform into freq once after we are done

  //clears the frame's contentsb so that it as good as new
  def clearFrame() : Unit = {
    relations = relations.map(x => (x._1,0))
  }
  def relationSize(relation: String): Int = {
    relations(relation)
  }
  def addEvent(event : Record) : Unit = {
    if (event.isEndMarker)
      return
    if(relations.contains(event.label))
      relations(event.label) += 1
    else
      relations += (event.label -> 1)
    for( i <- event.data.indices)
    {
      if(!valueOccurances.contains(event.label))
        valueOccurances += (event.label -> scala.collection.mutable.Map[(Int,Domain),Int]())
      if(!valueOccurances(event.label).contains(i,event.data(i)))
        valueOccurances(event.label) += ((i,event.data(i)) -> 0)
      valueOccurances(event.label)(i,event.data(i)) += 1
    }
  }
  def getRelInfo(relation: String): scala.collection.mutable.Map[(Int,Domain),Int] = {
    if(valueOccurances.contains(relation))
      valueOccurances(relation)
    else
      scala.collection.mutable.Map[(Int,Domain),Int]()
  }
}