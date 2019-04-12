package ch.ethz.infsec.monitor;

import java.util.Objects;

/*
Rules for POJO types
Flink recognizes a data type as a POJO type (and allows “by-name” field referencing) if the following conditions are fulfilled:

The class is public and standalone (no non-static inner class)
The class has a public no-argument constructor
All non-static, non-transient fields in the class (and all superclasses) are either public (and non-final) or have a public getter- and a setter- method that follows the Java beans naming conventions for getters and setters.
Note that when a user-defined data type can’t be recognized as a POJO type, it must be processed as GenericType and serialized with Kryo.

 */
public class DomainElement {
    private Long integralVal=null;
    private String stringVal=null;
    private Double floatVal=null;

    public DomainElement(){
    }
    private DomainElement(Long i){
        this.integralVal=Objects.requireNonNull(i,"null integral domain value");
    }
    private DomainElement(String s){
        this.stringVal=Objects.requireNonNull(s,"null string domain value");
    }
    private DomainElement(Double f){
        this.floatVal=Objects.requireNonNull(f,"null float domain value");
    }

    public Long getIntegralVal() { return integralVal; }

    public void setIntegralVal(Long integralVal) { this.integralVal = integralVal; }

    public String getStringVal() { return stringVal; }

    public void setStringVal(String stringVal) { this.stringVal = stringVal; }

    public Double getFloatVal() { return floatVal; }

    public void setFloatVal(Double floatVal) { this.floatVal = floatVal; }

    public static DomainElement integralVal(Long v) {return new DomainElement(v);}
    public static DomainElement stringVal(String v) {return new DomainElement(v);}
    public static DomainElement floatVal(Double  v) {return new DomainElement(v);}

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DomainElement that = (DomainElement) o;
        return Objects.equals(integralVal, that.integralVal) &&
                Objects.equals(stringVal, that.stringVal) &&
                Objects.equals(floatVal, that.floatVal);
    }

    @Override
    public int hashCode() {

        return Objects.hash(integralVal, stringVal, floatVal);
    }

    @Override
    public String toString() {
        return (integralVal==null?"":integralVal.toString()) +
               (stringVal==null?"":stringVal) +
               (floatVal==null?"":floatVal.toString());
    }
}
