package ch.eth.inf.infsec.autobalancer

import java.io.Serializable

import ch.eth.inf.infsec.slicer.Statistics
import ch.eth.inf.infsec.trace.{Domain, Record, Timestamp}


object Helpers
{
    def fuse(a:scala.collection.mutable.Map[(Int,Domain),Double],b:scala.collection.mutable.Map[(Int,Domain),Double]):scala.collection.mutable.Map[(Int,Domain),Double] = {
      a //todo
    }
    def heavyHitterFreq(value:Domain,freq:Double) : Boolean = {
      true
    }
}
class WindowStatistics extends Statistics with Serializable{
  var lastFrame = 0
  val framesPerSec = 30; //Todo: turn into config option
  val maxFrames = framesPerSec*5; //Todo: turn into config option
  var frames = Array.fill[FrameStatistic](maxFrames)(new FrameStatistic())
  var relations = scala.collection.mutable.Map[String,Double](); //todo: make param
  var heavyHitter = scala.collection.mutable.Map[(String,Int),Set[Domain]]()
  def recomputeFrameInfo() : Unit = {
            //recomputes relation sizes
            relations = relations.map(rel => (rel._1,frames.foldRight(0.0)((x,acc) => acc + x.relationSize(rel._1))));
            val temp = relations
              .map(rel => (rel._1,frames
                .foldRight(scala.collection.mutable.Map[(Int,Domain),Double]())((x,acc) => Helpers.fuse(acc,x.getRelInfo(rel._1)))
                .map(elm => (elm._1,elm._2 / rel._2))
                .filter(x => Helpers.heavyHitterFreq(x._1._2,x._2))))
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
  var frameTimestamp:Timestamp = 0//new trace.Timestamp();
  def addEvent(event : Record) : Unit = {
    frames(lastFrame).addEvent(event)
    if(event.timestamp > frameTimestamp + 1000/framesPerSec) {
      nextFrame()
      frameTimestamp = event.timestamp
    }
  }
}
class FrameStatistic extends Serializable
{
  var relations = scala.collection.mutable.Map[String,Double]();
  // (Name, Attribute, Value) -> Frequency
  var valueFreq = scala.collection.mutable.Map[String,scala.collection.mutable.Map[(Int,Domain),Double]]();
  //      var isProcessed = false; - todo: process opt, so that we store count first, then transform into freq once after we are done

  //clears the frame's contentsb so that it as good as new
  def clearFrame() : Unit = {
    relations = relations.map(x => (x._1,0.0))
  }
  def relationSize(relation: String): Double = {
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
      if(!valueFreq.contains(event.label))
        valueFreq += (event.label -> scala.collection.mutable.Map[(Int,Domain),Double]())
      if(!valueFreq(event.label).contains(i,event.data(i)))
        valueFreq(event.label) += ((i,event.data(i)) -> 0)
      valueFreq(event.label)(i,event.data(i)) += 1
    }
  }
  def getRelInfo(relation: String): scala.collection.mutable.Map[(Int,Domain),Double] = {
    if(valueFreq.contains(relation))
      valueFreq(relation)
    else
      scala.collection.mutable.Map[(Int,Domain),Double]()
  }
}