package ch.ethz.infsec.slicer

import java.util

import ch.ethz.infsec.monitor.Fact
import ch.ethz.infsec.policy.{Pred, VariableID, _}
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed
import org.apache.flink.util.Collector

import scala.collection.mutable

// NOTE(JS): Also performs basic filtering (constants).
abstract class DataSlicer extends FlatMapFunction[Fact, (Int, Fact)] with ListCheckpointed[String] {
  val formula: Formula

  // TODO(JS): Required for commands. Can be eliminated once elastic rescaling is implemented.
  val maxDegree: Int

  def degree: Int

  var pendingSlicer: String = _

  def setSlicer(fact: Fact): Unit = {
    pendingSlicer = fact.getArgument(0).asInstanceOf[String]
  }

  def stringify: String
  def unstringify(s: String): Unit

  def addSlicesOfValuation(valuation: Array[Any], slices: mutable.HashSet[Int])

  def slicesOfValuation(valuation: Array[Any]): collection.Set[Int] = {
    val slices = new mutable.HashSet[Int]()
    addSlicesOfValuation(valuation, slices)
    slices
  }

  // These arrays are reused in process() for performance.
  // We have to initialize them lazily, because they depend on properties provided by the implementing subclass.
  private var atoms: Array[Pred[VariableID]] = _
  private var valuation: Array[Any] = _

  def processCommand(fact: Fact, f: Collector[(Int, Fact)]): Unit = {
    if (fact.getName == "set_slicer") {
      setSlicer(fact)
    }

    // Send commands to all slices, including unused ones.
    var i = 0
    while (i < maxDegree) {
      f.collect((i, fact))
      i += 1
    }
  }

  def processEvent(fact: Fact, f: Collector[(Int, Fact)]): Unit = {
    if (atoms == null) {
      atoms = formula.atoms.toArray
      valuation = Array.fill(formula.freeVariables.size)(null)
    }

    // The end of a timepoint is always broadcast to all slices.
    if (fact.isTerminator) {
      var i = 0
      // Only send databases to slices that are in used.
      // This is important for soundness if the verdict filtering is disabled!
      while (i < degree) {
        f.collect((i, fact))
        i += 1
      }
      return
    }

    val slices = new mutable.HashSet[Int]()

    // The set of relevant slices is the union of the slices for each atom.
    var i = 0
    while (i < atoms.length) {
      val atom = atoms(i)
      if (atom.relation == fact.getName && atom.args.lengthCompare(fact.getArity) == 0) {
        var matches = true
        var j = 0
        while (j < valuation.length) {
          valuation(j) = null
          j += 1
        }

        // Determine whether the tuple matches the atom, and compute the induced valuation of free variables.
        j = 0
        while (j < fact.getArity) {
          val term = atom.args(j)
          val value = fact.getArgument(j)
          term match {
            case Const(c) if c != value => matches = false
            case Var(x) if x.isFree =>
              if (valuation(x.freeID) == null)
                valuation(x.freeID) = value
              else if (valuation(x.freeID) != value)
                matches = false
            case _ => ()
          }
          j += 1
        }

        if (matches)
          addSlicesOfValuation(valuation, slices)
      }
      i += 1
    }

    slices.foreach(s => f.collect(s, fact))
  }

  def requiresFilter: Boolean

  def filterVerdict(slice: Int, verdict: Fact): Boolean

  override def flatMap(fact: Fact, collector: Collector[(Int, Fact)]): Unit = {
    if (fact.isMeta) {
      processCommand(fact, collector)
    } else {
      processEvent(fact, collector)
    }
  }

  override def snapshotState(checkpointId: Long, timestamp: Long): util.List[String] =
    util.Collections.singletonList(if (pendingSlicer == null) stringify else pendingSlicer)

  override def restoreState(state: util.List[String]): Unit = {
    assert(state.size() == 1)
    unstringify(state.get(0))
  }
}
