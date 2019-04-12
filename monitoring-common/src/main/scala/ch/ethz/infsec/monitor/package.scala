package ch.ethz.infsec


package object monitor {

  implicit class Domain(e:DomainElement){
    val elem:DomainElement = e
    def getIntegralVal:Long = elem.getIntegralVal
    def getStringVal:String = elem.getStringVal
    def getFloatVal:Double = elem.getFloatVal

    override def equals(o:Any): Boolean = o match {
      case Domain(el) => elem.equals(el)
      case _ => false
    }
    override def hashCode: Int = elem.hashCode
    override def toString: String = elem.toString
  }

  object Domain {
    def apply(i:Long): Domain = DomainElement.integralVal(i)
    def apply(s:String): Domain = DomainElement.stringVal(s)
    def apply(f:Double): Domain = DomainElement.floatVal(f)

    def unapply(arg: Domain): Option[DomainElement] = Option(arg.elem)
  }

  object IntegralValue {
    def apply(v: Long): Domain = Domain(v)
    def unapply(d: Domain): Option[Long] = Option(d.getIntegralVal)
  }
  object StringValue {
    def apply(v: String): Domain = Domain(v)
    def unapply(d: Domain): Option[String] = Option(d.getStringVal)
  }
  object FloatValue {
    def apply(v: Double): Domain = Domain(v)
    def unapply(d: Domain): Option[Double] = Option(d.getFloatVal)
  }


  implicit def longToDomain(value: Long): Domain = IntegralValue(value)
  implicit def intToDomain(value: Int): Domain = IntegralValue(value)
  implicit def stringToDomain(value: String): Domain = StringValue(value)
  implicit def floatToDomain(value: Float): Domain = FloatValue(value)
  implicit def doubleToDomain(value: Double): Domain = FloatValue(value)

}
