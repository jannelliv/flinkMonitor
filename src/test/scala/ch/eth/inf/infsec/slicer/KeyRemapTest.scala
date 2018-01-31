package ch.eth.inf.infsec.slicer

import org.apache.flink.runtime.state.KeyGroupRangeAssignment
import org.scalatest.{FunSuite, Matchers}

class KeyRemapTest extends FunSuite with Matchers {


  test("Remap"){

    val numKeys = 4
    val keys = 0 until numKeys
    val mapping = ColissionlessKeyGenerator.getMapping(numKeys)
    keys foreach  (mapping.isDefinedAt(_) shouldBe true)
    keys.foldLeft(Set[Int]())((s:Set[Int],i)=>s.+(mapping(i))).size shouldBe numKeys
  }

}
