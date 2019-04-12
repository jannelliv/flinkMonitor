package ch.ethz.infsec.monitor;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;


public class DomainElementTest {

    DomainElement empty;
    DomainElement i;
    DomainElement s;
    DomainElement f;

    @Before
    public void create(){

        empty = new DomainElement();
        i = DomainElement.integralVal(1L);
        s = DomainElement.stringVal("a");
        f = DomainElement.floatVal(1.0);

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
    public void testCreateFail1(){
        DomainElement d = DomainElement.integralVal(null);
    }

    @Test(expected = NullPointerException.class)
    public void testCreateFail2(){
        DomainElement d = DomainElement.stringVal(null);
    }

    @Test(expected = NullPointerException.class)
    public void testCreateFail3(){
        DomainElement d = DomainElement.floatVal(null);
    }


}
