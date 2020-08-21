package ch.ethz.infsec.autobalancer

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream, Serializable}
import java.util.Base64

import ch.ethz.infsec.autobalancer.Helpers.{Domain, Histogram}
import ch.ethz.infsec.monitor.Fact

import scala.collection.mutable

object StatsHistogram {
  def fromBase64(s: String): StatsHistogram = {
    val data = Base64.getDecoder.decode(s)
    val ois = new ObjectInputStream(new ByteArrayInputStream(data))
    val o = ois.readObject
    ois.close()
    o.asInstanceOf[StatsHistogram]
  }
}

trait StatsHistogram extends Serializable {
  type Histogram = mutable.Map[(String,Int),mutable.Map[Domain, (Int, Int)]]

  def merge(statsHistogram: StatsHistogram): Unit

  def relationSize(relation: String) : Double

  def heavyHitter(relation: String, attribute: Int): Set[Domain]

  def toBase64: String = {
    val baos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(this)
    oos.close()
    Base64.getEncoder.encodeToString(baos.toByteArray)
  }
}

case class ConstantHistogram() extends StatsHistogram with Serializable {
  override def merge(statsHistogram: StatsHistogram): Unit = throw new RuntimeException("INVARIANT: Constant histograms can not be merged")

  override def relationSize(relation: String): Double = 100.0

  override def heavyHitter(relation: String, attribute: Int): Set[Domain] = Set()
}

case class SimpleHistogram(sizes: (String, Double)*) extends StatsHistogram with Serializable {
  val map: Map[String, Double] = Map(sizes:_*)

  override def merge(statsHistogram: StatsHistogram): Unit = throw new RuntimeException("INVARIANT: Simple histograms can not be merged")

  override def relationSize(relation: String): Double = map(relation)

  override def heavyHitter(relation: String, attribute: Int): Set[Domain] = Set()
}

case class NormalHistogram(var h: Histogram, deg: Int) extends StatsHistogram with Serializable {
  var heavyHitters: collection.Map[(String,Int),Set[Domain]] = _
  var heaveHittersClean: Boolean = false
  var relationSizes: collection.Map[String, Int] = _
  var relationSizesClean: Boolean = false

  override def merge(statsHistogram: StatsHistogram): Unit = {
    statsHistogram match {
      case ConstantHistogram() => throw new RuntimeException("merging constant histogram")
      case SimpleHistogram(_) => throw new RuntimeException("merging simple histogram")
      case NormalHistogram(h2, deg2) =>
        if (deg != deg2) {
          throw new RuntimeException("trying to merge histograms with different degree")
        } else {
          h = Helpers.mergeHistograms(h, h2)
        }
      case _ => throw new RuntimeException("should not happen")
    }
    heaveHittersClean = false
    relationSizesClean = false
  }

  override def relationSize(relation: String): Double = {
    if (!heaveHittersClean) {
      heavyHitters = Helpers.heavyHittersFromHistogram(h, deg)
      heaveHittersClean = true
    }
    if (!relationSizesClean) {
      relationSizes = Helpers.relationSizesFromHistogram(h, deg)
      relationSizesClean = true
    }
    relationSizes.getOrElse(relation, 0).toDouble
  }

  override def heavyHitter(relation: String, attribute: Int): Set[Domain] = {
    if (!heaveHittersClean) {
      heavyHitters = Helpers.heavyHittersFromHistogram(h, deg)
      heaveHittersClean = true
    }
    heavyHitters.getOrElse((relation,attribute),Set[Domain]())
  }
}

object Helpers
{
  type Domain = Any
  type Histogram = mutable.Map[(String,Int),mutable.Map[Domain, (Int, Int)]]

    def fuseGen[K,V](a : scala.collection.mutable.Map[K,V],b : scala.collection.mutable.Map[K,V], default : V, add : ((V,V) => V)): scala.collection.mutable.Map[K,V] = {
      a ++ b.map { case (k,v) => k -> add(v,a.getOrElse(k,default))}
    }

    def fuse(a:scala.collection.mutable.Map[(Int,Domain),Int],b:scala.collection.mutable.Map[(Int,Domain),Int]):scala.collection.mutable.Map[(Int,Domain),Int] = {
      a ++ b.map { case (k,v) => k -> (v + a.getOrElse(k,0))}
    }

    def heavyHitterFreq(valuePerPosFreq:Double, degree : Int) : Boolean = {
      valuePerPosFreq > (1/(2*(degree.toDouble)))
    }

    def mergeMaps[K, V, V2](m1: mutable.Map[K, V], m2: mutable.Map[K, V], f : (K, Option[V], Option[V]) => Option[V2]) : mutable.Map[K,V2] = {
      val ret = mutable.Map[K, V2]()
      for ((k, v) <- m1) {
        f(k, Some(v), m2.get(k)).foreach(v2 => ret += (k -> v2))
      }
      for ((k, v) <- m2) {
        m1.get(k) match {
          case Some(_) => ()
          case None => f(k, None, Some(v)).foreach(v2 => ret += (k -> v2))
        }
      }
      ret
    }

    def mergeHistograms(h1: Histogram, h2: Histogram) : Histogram = {
      val f2 = (key: Domain, v1: Option[(Int, Int)], v2: Option[(Int, Int)]) => {
        (v1, v2) match {
          case (None, None) => None
          case (Some(v1), None) => Some(v1)
          case (None, Some(v2)) => Some(v2)
          case (Some(v1), Some(v2)) =>
            Some((v1._1 + v2._1, v1._2 + v2._2))
        }
      } : Option[(Int, Int)]
      val f1 = (key: (String,Int), v1: Option[mutable.Map[Domain, (Int, Int)]], v2: Option[mutable.Map[Domain, (Int, Int)]]) => {
        (v1, v2) match {
          case (None, None) => None
          case (Some(m1), None) => Some(m1)
          case (None, Some(m2)) => Some(m2)
          case (Some(m1), Some(m2)) =>
            Some(mergeMaps(m1, m2, f2))
        }
      } : Option[mutable.Map[Domain, (Int, Int)]]

      mergeMaps(h1, h2, f1)
    }

    def mergeHistogramsList(hists: List[Histogram]) : Histogram = {
      val base: Histogram = mutable.Map()
      hists.foldLeft(base)((h1, h2) => mergeHistograms(h1, h2))
    }

    def heavyHittersFromHistogram(h: Histogram, deg: Int) : collection.Map[(String,Int),Set[Domain]] = {
      h.mapValues(k => {
        val set = mutable.Set[Domain]()
        k
          .filter(l => heavyHitterFreq(l._2._1 / l._2._2.toDouble, deg))
          .foreach(l => set.add(l._1))
        set.toSet
      })
    }

    def relationSizesFromHistogram(h: Histogram, deg: Int): collection.Map[String, Int] = {
      h.toSeq.groupBy(_._1._1).mapValues(
        _.foldLeft(0)((sum, l) => sum + l._2.mapValues(_._2).values.sum))
    }
}
//todo: degree doesn't really belong here, it'd be better if heavyHitterFreq were given as a function in the constructor
class WindowStatistics(maxFrames : Int, timestampDeltaBetweenFrames : Double, degree : Int) extends Serializable {
  type Domain = Any

  var lastFrame = 0
//  val timestampDeltaBetweenFrames = 0.033;
//  val maxFrames = 150;
  var frames = Array.fill[FrameStatistic](maxFrames)(new FrameStatistic())
  var relations = mutable.Map[String,Int](); //todo: make param

  var histogram = mutable.Map[(String,Int),mutable.Map[Domain, (Int, Int)]]()
  def recomputeFrameInfo() : Unit = {
    //we can optimize here:
    //   - instead of recomputing relations completely, we can do the delta. namely we subtract frames(lastFrame) from relations and add frames(lastFrame-1) to relations
    //   - for heavy hitters we can do two opts:
    //   - first: we take all the ones we have already and multiply with the old relations (to get occurances again instead of freq)
    //            then we subtract and add like we did with relations
    //    ... this gets complicated, postpone until needed
    //recomputes relation sizes
//  relations = relations.map(rel => (rel._1,frames.foldRight(0)((x,acc) => acc + x.relationSize(rel._1))));
    relations = frames.foldRight(scala.collection.mutable.Map[String,Int]())((f,acc) => Helpers.fuseGen[String,Int](f.relations,acc,0,(a,b) => (a+b)))
    histogram = mutable.Map()
    relations
      .map(rel => (rel._1,frames
        .foldRight(scala.collection.mutable.Map[(Int,Domain),Int]())((x,acc) => Helpers.fuse(acc,x.getRelInfo(rel._1)))
        .map(elm => (elm._1, (elm._2, rel._2)))))
      .foreach(k => {
        k._2.foreach(l => {
          histogram.getOrElseUpdate((k._1, l._1._1), mutable.Map())
          histogram((k._1, l._1._1)) += (l._1._2 -> l._2)
        })
      })
    /*heavyHitter = scala.collection.mutable.Map[(String,Int),Set[Domain]]()
    temp.foreach(x => {
      x._2.foreach( y => {
        heavyHitter.getOrElseUpdate((x._1,y._1._1),Set[Domain]())
        heavyHitter(x._1, y._1._1) += y._1._2
      })
    })*/
  }
  def nextFrame() : Unit = {
    lastFrame += 1
    if(lastFrame >= maxFrames)
      lastFrame = 0
    recomputeFrameInfo()
    frames(lastFrame).clearFrame()
  }

  //var justHadRollover = false

  //todo: the timestamp is atm nonsense, needs fixing
  //var frameTimestamp:Double = 0//new trace.Timestamp();
  //var first = true;
  def addEvent(event : Fact) : Unit = {
    /*if(first) {
      frameTimestamp = event.getTimestamp
      first = false
    }*/
    frames(lastFrame).addEvent(event)
    //justHadRollover = false
    /*while(event.getTimestamp >= frameTimestamp + timestampDeltaBetweenFrames) {
      nextFrame()
      frameTimestamp = frameTimestamp + timestampDeltaBetweenFrames
      justHadRollover = true
    }*/
  }

  /*def hadRollover() : Boolean = {
    return justHadRollover
  }*/

  def getHistogram: Histogram = histogram
}

class FrameStatistic extends Serializable
{
  type Domain = Any

  var relations = scala.collection.mutable.Map[String,Int]();
  // (Name, Attribute, Value) -> Frequency
  var valueOccurances = scala.collection.mutable.Map[String,scala.collection.mutable.Map[(Int,Domain),Int]]();
  //      var isProcessed = false; - todo: process opt, so that we store count first, then transform into freq once after we are done

  //clears the frame's contents so that it as good as new
  def clearFrame() : Unit = {
    //relations = relations.map(x => (x._1,0))
    relations = scala.collection.mutable.Map[String,Int]()
    valueOccurances = scala.collection.mutable.Map[String,scala.collection.mutable.Map[(Int,Domain),Int]]()
  }
  def relationSize(relation: String): Int = {
    relations(relation)
  }
  def addEvent(event : Fact) : Unit = {
    if (event.isTerminator)
      return
    if(relations.contains(event.getName))
      relations(event.getName) += 1
    else
      relations += (event.getName -> 1)
    for( i <- Range(0,event.getArguments.size))
    {
      if(!valueOccurances.contains(event.getName))
        valueOccurances += (event.getName -> scala.collection.mutable.Map[(Int,Domain),Int]())
      if(!valueOccurances(event.getName).contains(i,event.getArguments.get(i)))
        valueOccurances(event.getName) += ((i,event.getArguments.get(i)) -> 0)
      valueOccurances(event.getName)(i,event.getArguments.get(i)) += 1
    }
  }
  def getRelInfo(relation: String): scala.collection.mutable.Map[(Int,Domain),Int] = {
    if(valueOccurances.contains(relation))
      valueOccurances(relation)
    else
      scala.collection.mutable.Map[(Int,Domain),Int]()
  }
}
