package ch.ethz.infsec.slicer

import org.scalatest.matchers.should.Matchers
import org.scalatest.funsuite.AnyFunSuite

class KeyRemapTest extends AnyFunSuite with Matchers {


  test("Remap"){

    val numKeys = 4
    val keys = 0 until numKeys
    val mapping = ColissionlessKeyGenerator.getMapping(numKeys)
    keys foreach  (mapping.isDefinedAt(_) shouldBe true)
    keys.foldLeft(Set[Int]())((s:Set[Int],i)=>s.+(mapping(i))).size shouldBe numKeys
  }

}
