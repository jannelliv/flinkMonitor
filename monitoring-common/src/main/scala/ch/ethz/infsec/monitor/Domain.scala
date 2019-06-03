package ch.ethz.infsec.monitor

import scala.language.implicitConversions


class Domain(e:DomainElement) extends Serializable {
  val elem:DomainElement = e
  /*
    Exlicit if-then-else check is used to avoid implicit conversion between
    java.lang.Long and scala.lang.Long
   */
  def getIntegralVal:Option[Long] = if (elem.getIntegralVal == null) None else Some(elem.getIntegralVal )
  def getStringVal:Option[String] = Option(elem.getStringVal)
  def getFloatVal:Option[Double] = if (elem.getFloatVal == null) None else Some(elem.getFloatVal)

  override def equals(o:Any): Boolean = o match {
    case Domain(el) => elem.equals(el)
    case _ => false
  }
  override def hashCode: Int = elem.hashCode
  override def toString: String = elem.toString
}

object Domain {

  def apply(e:DomainElement): Domain = new Domain(e)
  def apply(i:Long): Domain = DomainElement.integralVal(i)
  def apply(s:String): Domain = DomainElement.stringVal(s)
  def apply(f:Double): Domain = DomainElement.floatVal(f)
  def unapply(arg: Domain): Option[DomainElement] = Option(arg.elem)


  implicit def domainElementToDomain(value: DomainElement): Domain = Domain(value)
  implicit def longToDomain(value: Long): Domain = IntegralValue(value)
  implicit def intToDomain(value: Int): Domain = IntegralValue(value)
  implicit def stringToDomain(value: String): Domain = StringValue(value)
  implicit def floatToDomain(value: Float): Domain = FloatValue(value)
  implicit def doubleToDomain(value: Double): Domain = FloatValue(value)

}

object IntegralValue {
  def apply(v: Long): Domain = Domain(v)
  def unapply(d: Domain): Option[Long] = d.getIntegralVal
}
object StringValue {
  def apply(v: String): Domain = Domain(v)
  def unapply(d: Domain): Option[String] = d.getStringVal
}
object FloatValue {
  def apply(v: Double): Domain = Domain(v)
  def unapply(d: Domain): Option[Double] = d.getFloatVal
}




