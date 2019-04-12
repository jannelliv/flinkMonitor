package ch.ethz.infsec.monitor;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;


public class DomainElementTest {

    DomainElement empty;
    DomainElement i;
    DomainElement s;
    DomainElement f;

    DomainElement i1;
    DomainElement i2;
    DomainElement i3;

    DomainElement s1;
    DomainElement s2;
    DomainElement s3;

    DomainElement f1;
    DomainElement f2;
    DomainElement f3;

    @Before
    public void create(){

        empty = new DomainElement();
        i = DomainElement.integralVal(1L);
        s = DomainElement.stringVal("a");
        f = DomainElement.floatVal(1.0);

        i1 = DomainElement.integralVal(1L);
        i2 = DomainElement.integralVal(2L);
        i3 = DomainElement.integralVal(2L);

        s1 = DomainElement.stringVal("a");
        s2 = DomainElement.stringVal("b");
        s3 = DomainElement.stringVal("b");

        f1 = DomainElement.floatVal(1.0);
        f2 = DomainElement.floatVal(2.0);
        f3 = DomainElement.floatVal(2.0);


    }

    @Test
    public void testCreateSuccess(){

        assertEquals(null, empty.getIntegralVal());
        assertEquals(null, empty.getStringVal());
        assertEquals(null, empty.getFloatVal());

        assertEquals(Long.valueOf(1L), i.getIntegralVal());
        assertEquals(null, i.getStringVal());
        assertEquals(null, i.getFloatVal());

        assertEquals(null, s.getIntegralVal());
        assertEquals("a", s.getStringVal());
        assertEquals(null, s.getFloatVal());

        assertEquals(null, f.getIntegralVal());
        assertEquals(null, f.getStringVal());
        assertEquals(Double.valueOf(1.0), f.getFloatVal());

    }

    @Test(expected = NullPointerException.class)
    public void testCreateFailInt(){
        DomainElement d = DomainElement.integralVal(null);
    }

    @Test(expected = NullPointerException.class)
    public void testCreateFailString(){
        DomainElement d = DomainElement.stringVal(null);
    }

    @Test(expected = NullPointerException.class)
    public void testCreateFailFloat(){
        DomainElement d = DomainElement.floatVal(null);
    }

    @Test(expected = IllegalStateException.class)
    public void testMutateFailInt(){ i.setIntegralVal(2L); }

    @Test(expected = IllegalStateException.class)
    public void testMutateFailString(){ s.setStringVal("b"); }

    @Test(expected = IllegalStateException.class)
    public void testMutateFailFloat(){ f.setFloatVal(2.0); }

    @Test
    public void testMutateSuccess(){ empty.setFloatVal(2.0); }

    @Test
    public void testEquals(){

        assertEquals(i1,i1);
        assertEquals(s1,s1);
        assertEquals(f1,f1);

        assertNotEquals(i1,i2);
        assertNotEquals(s1,s2);
        assertNotEquals(f1,f2);

        assertNotSame(i2,i3);
        assertEquals(i2,i3);

        assertNotSame(s2,s3);
        assertEquals(s2,s3);

        assertNotSame(f2,f3);
        assertEquals(f2,f3);

    }

    @Test
    public void testHashCode(){

        assertNotSame(i2,i3);
        assertEquals(i2.hashCode(),i3.hashCode());

        assertNotSame(s2,s3);
        assertEquals(s2.hashCode(),s3.hashCode());

        assertNotSame(f2,f3);
        assertEquals(f2.hashCode(),f3.hashCode());

    }


}
