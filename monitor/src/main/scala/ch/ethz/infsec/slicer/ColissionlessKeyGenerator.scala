package ch.ethz.infsec.slicer

import org.apache.flink.runtime.state.KeyGroupRangeAssignment
import org.apache.flink.util.MathUtils

import scala.collection.mutable

/**
  * ColissionlessKeyGenerator generates custom keys that
  * do not create collisions in the flink keyBy implementation
  * Obviously, this implementation depends on the internals of
  * Flink and will be broken if something changes.
  *
  * @param partitions actual number of partitions
  * @param maxPartitions max number of partitions
  */
class ColissionlessKeyGenerator(val partitions: Int,
                   val maxPartitions: Int) {

  def this(partitions: Int) = this(partitions, 128)

  val ids = Stream.from(1).iterator
  val cache = mutable.HashMap[Int, mutable.Queue[Int]]()

  def next(targetPartition: Int): Int = {
    val queue = cache.getOrElseUpdate(targetPartition, mutable.Queue[Int]())
    if (queue.size == 0) {
      var found = false
      while (!found) {
        val id = ids.next
        val partition =
          (MathUtils.murmurHash(id) % maxPartitions) * partitions / maxPartitions

        cache
          .getOrElseUpdate(partition, mutable.Queue[Int]())
          .enqueue(id)

        if (partition == targetPartition) {
          found = true
        }
      }
    }
    queue.dequeue()
  }
}

object ColissionlessKeyGenerator{

  def getMapping(numKeys:Int,parallelism:Int,numPartitions:Int):PartialFunction[Int,Int] = {
    val dp =  KeyGroupRangeAssignment.computeDefaultMaxParallelism(parallelism)
    val keyGenerator = new ColissionlessKeyGenerator(numPartitions, dp)
    val map = mutable.HashMap[Int,Int]()
    (0 until numKeys) foreach  { k=> map.update(k,keyGenerator.next(k)) }
    return map
  }

  def getMapping(numKeys:Int):PartialFunction[Int,Int] = {
    getMapping(numKeys,numKeys,numKeys)
  }
}