package ch.ethz.infsec.slicer

import ch.ethz.infsec.Processor
import ch.ethz.infsec.monitor.Domain
import ch.ethz.infsec.policy._
import ch.ethz.infsec.trace.{CommandRecord, EventRecord, Record, Tuple}
import ch.ethz.infsec.policy.{Pred, VariableID}

import scala.collection.mutable

// NOTE(JS): Also performs basic filtering (constants).
abstract class DataSlicer extends Processor[Record, (Int, Record)] {
  override type State = Array[Byte]

  val formula: Formula

  // TODO(JS): Required for commands. Can be eliminated once elastic rescaling is implemented.
  val maxDegree: Int
  val degree: Int

  var pendingSlicer: String = _

  def addSlicesOfValuation(valuation: Array[Domain], slices: mutable.HashSet[Int])

  def slicesOfValuation(valuation: Array[Domain]): collection.Set[Int] = {
    val slices = new mutable.HashSet[Int]()
    addSlicesOfValuation(valuation, slices)
    slices
  }

  // These arrays are reused in process() for performance.
  // We have to initialize them lazily, because they depend on properties provided by the implementing subclass.
  private var atoms: Array[Pred[VariableID]] = _
  private var valuation: Array[Domain] = _

  override def process(record: Record, f: ((Int, Record)) => Unit) {
    record match {
      case CommandRecord(record.command, record.parameters) => processCommand(record, f)
      case EventRecord(record.timestamp, record.label, record.data) =>   processEvent(record, f)
    }
  }

  def updateState(state: Array[Byte]): Unit = ()

  def setSlicer(record: Record): Unit = {
    pendingSlicer = record.parameters
  }

  def processCommand(record: Record, f: ((Int, Record)) => Unit): Unit ={
    var i = 0

    setSlicer(record)

    // Send commands to all slices, including unused ones.
    while (i < maxDegree) {
      f((i, record))
      i += 1
    }
  }

  def processEvent(record: Record, f: ((Int, Record)) => Unit): Unit = {
    if (atoms == null) {
      atoms = formula.atoms.toArray
      valuation = Array.fill(formula.freeVariables.size)(null)
    }

    // The end of a timepoint is always broadcast to all slices.
    if (record.isEndMarker) {
      var i = 0
      // Only send databases to slices that are in used.
      // This is important for soundness if the verdict filtering is disabled!
      while (i < degree) {
        f((i, record))
        i += 1
      }
      return
    }

    val slices = new mutable.HashSet[Int]()

    // The set of relevant slices is the union of the slices for each atom.
    var i = 0
    while (i < atoms.length) {
      val atom = atoms(i)
      if (atom.relation == record.label && atom.args.lengthCompare(record.data.length) == 0) {
        var matches = true
        var j = 0
        while (j < valuation.length) {
          valuation(j) = null
          j += 1
        }

        // Determine whether the tuple matches the atom, and compute the induced valuation of free variables.
        j = 0
        while (j < record.data.length) {
          val term = atom.args(j)
          val value = record.data(j)
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

    slices.foreach(s => f(s, record))
  }

  override def getState(): State

  override def restoreState(state: Option[State]): Unit

  override def terminate(f: ((Int, Record)) => Unit) { }

//  override def apply(source: Stream[Event])(implicit in:TypeInfo[Event], out:TypeInfo[(Int, Event)]): source.Self[(Int, Event)] = {
//    source.flatMap(e => {
//      // TODO(JS): For efficiency, consider transposing the outermost layers of `slices`: relation -> slice id -> structure
//      val slices = Array.fill(degree) {
//        val slice = new mutable.HashMap[String, ArrayBuffer[Tuple]]()
//        for (relation <- e.structure.keys)
//          slice(relation) = new ArrayBuffer()
//        slice
//      }
//
//      for ((relation, data) <- e.structure)
//        for (tuple <- data)
//          for (i <- slicesOfTuple(relation, tuple))
//            slices(i)(relation) += tuple
//
//      for (i <- slices.indices)
//        yield (i, Event(e.timestamp, slices(i)))
//    })
//  }

  def requiresFilter: Boolean

  def mkVerdictFilter(slice: Int)(verdict: Tuple): Boolean
}
