package ch.ethz.infsec;

import ch.ethz.infsec.formula.JavaGenFormula;
import ch.ethz.infsec.formula.visitor.Init0;
import ch.ethz.infsec.monitor.*;
import ch.ethz.infsec.policy.GenFormula;
import ch.ethz.infsec.policy.Policy;
import ch.ethz.infsec.policy.Prev;
import ch.ethz.infsec.policy.VariableID;
import ch.ethz.infsec.util.Assignment;
import ch.ethz.infsec.util.ParsingFunction;
import ch.ethz.infsec.trace.parser.MonpolyTraceParser;
import ch.ethz.infsec.util.PipelineEvent;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.operators.StreamFlatMap;
import org.apache.flink.streaming.api.operators.co.CoStreamFlatMap;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.TwoInputStreamOperatorTestHarness;
import org.apache.flink.util.Collector;
import static org.junit.Assert.assertArrayEquals;
import org.junit.Before;
import org.junit.Test;
import scala.io.Codec;
import scala.io.Source;
import scala.util.Either;

import java.util.*;
import java.util.stream.Collectors;

import static ch.ethz.infsec.formula.JavaGenFormula.convert;
import static org.junit.Assert.assertEquals;


public class TestHierarchy {

    //testParsingWithFlink()
    private OneInputStreamOperatorTestHarness<String, Fact> testHarness;

    //testPred(), testPred2(),
    private OneInputStreamOperatorTestHarness<Fact, PipelineEvent> testHarnessPred;
    private OneInputStreamOperatorTestHarness<Fact, PipelineEvent> testHarnessPredConst;

    //testRelTrueFalse()
    private OneInputStreamOperatorTestHarness<Fact, PipelineEvent> testHarnessRel;

    //testEventually()
    private OneInputStreamOperatorTestHarness<Fact, PipelineEvent> testHarnessPredEv;
    private OneInputStreamOperatorTestHarness<PipelineEvent, PipelineEvent> testHarnessEv;

    //testOnce()
    private OneInputStreamOperatorTestHarness<Fact, PipelineEvent> testHarnessPredOnce;
    private OneInputStreamOperatorTestHarness<PipelineEvent, PipelineEvent> testHarnessOnce;



    //testAnd()
    private OneInputStreamOperatorTestHarness<Fact, PipelineEvent> testHarnessPred1;
    private OneInputStreamOperatorTestHarness<Fact, PipelineEvent> testHarnessPred2;
    private OneInputStreamOperatorTestHarness<Fact, PipelineEvent> testHarnessPred1fv;
    private OneInputStreamOperatorTestHarness<Fact, PipelineEvent> testHarnessPred2fv;
    private TwoInputStreamOperatorTestHarness<PipelineEvent, PipelineEvent, PipelineEvent> testHarnessAnd;
    private TwoInputStreamOperatorTestHarness<PipelineEvent, PipelineEvent, PipelineEvent> testHarnessAndFV;


    //testOr()
    private OneInputStreamOperatorTestHarness<Fact, PipelineEvent> testHarnessPred1Or;
    private OneInputStreamOperatorTestHarness<Fact, PipelineEvent> testHarnessPred2Or;
    private TwoInputStreamOperatorTestHarness<PipelineEvent, PipelineEvent, PipelineEvent> testHarnessOr;



    @Before
    public void setUp() throws Exception{



    }

    @Test
    public void testMain() throws Exception{
        ////((ONCE[0,10) A(a,b)) AND B(a,c)) AND EVENTUALLY[0,10) C(a,d)
        Either<String, GenFormula<VariableID>> a = Policy.read("((ONCE[0,10) A(a,b)) AND B(a,c)) AND EVENTUALLY[0,10) C(a,d)");
        if(a.isLeft()){
            throw new ExceptionInInitializerError();
        }
        GenFormula<VariableID> formula = a.right().get();
        JavaGenFormula convFormula = convert(formula);
        Mformula mformula = (convert(formula)).accept(new Init0(formula.freeVariablesInOrder()));
        //Mformula mformula = (convert(formula)).accept(new Init0(formula.freeVariablesInOrder()));

    }



}

