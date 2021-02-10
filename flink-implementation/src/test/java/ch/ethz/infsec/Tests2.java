package ch.ethz.infsec;

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
import scala.util.Either;

import java.util.*;
import java.util.stream.Collectors;

import static ch.ethz.infsec.formula.JavaGenFormula.convert;
import static org.junit.Assert.assertEquals;


public class Tests2 {

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


    //testPrev()
    private OneInputStreamOperatorTestHarness<Fact, PipelineEvent> testHarnessPredPrev;
    private OneInputStreamOperatorTestHarness<PipelineEvent, PipelineEvent> testHarnessPrev;

    //testNext()
    private OneInputStreamOperatorTestHarness<Fact, PipelineEvent> testHarnessPredNext;
    private OneInputStreamOperatorTestHarness<PipelineEvent, PipelineEvent> testHarnessNext;

    //testAnd()
    private OneInputStreamOperatorTestHarness<Fact, PipelineEvent> testHarnessPred1;
    private OneInputStreamOperatorTestHarness<Fact, PipelineEvent> testHarnessPred2;
    private TwoInputStreamOperatorTestHarness<PipelineEvent, PipelineEvent, PipelineEvent> testHarnessAnd;

    //testOr()
    private OneInputStreamOperatorTestHarness<Fact, PipelineEvent> testHarnessPred1Or;
    private OneInputStreamOperatorTestHarness<Fact, PipelineEvent> testHarnessPred2Or;
    private TwoInputStreamOperatorTestHarness<PipelineEvent, PipelineEvent, PipelineEvent> testHarnessOr;

    //testSince()
    private OneInputStreamOperatorTestHarness<Fact, PipelineEvent> testHarnessPred1Since;
    private OneInputStreamOperatorTestHarness<Fact, PipelineEvent> testHarnessPred2Since;
    private TwoInputStreamOperatorTestHarness<PipelineEvent, PipelineEvent, PipelineEvent> testHarnessSince;

    //testUntil()
    private OneInputStreamOperatorTestHarness<Fact, PipelineEvent> testHarnessPred1Until;
    private OneInputStreamOperatorTestHarness<Fact, PipelineEvent> testHarnessPred2Until;
    private TwoInputStreamOperatorTestHarness<PipelineEvent, PipelineEvent, PipelineEvent> testHarnessUntil;

    //testExists()
    private OneInputStreamOperatorTestHarness<Fact, PipelineEvent> testHarnessPredExists;
    private OneInputStreamOperatorTestHarness<PipelineEvent, PipelineEvent> testHarnessExists;



    @Before
    public void setUp() throws Exception{

        //testPred(), firstExample()
        Either<String, GenFormula<VariableID>> a = Policy.read("publish(r)");
        GenFormula<VariableID> formula = a.right().get();
        FlatMapFunction<Fact, PipelineEvent> statefulFlatMapFunctionPred = (MPred) (convert(formula)).accept(new Init0(formula.freeVariablesInOrder()));
        testHarnessPred = new OneInputStreamOperatorTestHarness<>(new StreamFlatMap<>(statefulFlatMapFunctionPred));
        testHarnessPred.open();

        //testPred2(),
        Either<String, GenFormula<VariableID>> constPred = Policy.read("publish(163)");
        GenFormula<VariableID> constPredFormula = constPred.right().get();
        FlatMapFunction<Fact, PipelineEvent> statefulFlatMapFunctionPredConst = (MPred) (convert(constPredFormula)).accept(new Init0(constPredFormula.freeVariablesInOrder()));
        testHarnessPredConst = new OneInputStreamOperatorTestHarness<>(new StreamFlatMap<>(statefulFlatMapFunctionPredConst));
        testHarnessPredConst.open();

        //testRelTrueFalse()
        Either<String, GenFormula<VariableID>> b = Policy.read("TRUE");
        GenFormula<VariableID> relFormula = b.right().get();
        FlatMapFunction<Fact, PipelineEvent> statefulFlatMapFunctionRel = (MRel) (convert(relFormula)).accept(new Init0(relFormula.freeVariablesInOrder()));
        testHarnessRel = new OneInputStreamOperatorTestHarness<>(new StreamFlatMap<>(statefulFlatMapFunctionRel));
        testHarnessRel.open();

        //testParsingWithFlink()
        FlatMapFunction<String, Fact> statefulFlatMapFunction = new ParsingFunction(new MonpolyTraceParser());
        testHarness = new OneInputStreamOperatorTestHarness<>(new StreamFlatMap<>(statefulFlatMapFunction));
        testHarness.open();


        //testPrev()
        Either<String, GenFormula<VariableID>> pubPred = Policy.read("publish(160)");
        GenFormula<VariableID> formulaPub = pubPred.right().get();
        FlatMapFunction<Fact, PipelineEvent> statefulFlatMapFunctionPredPrev = (MPred) (convert(formulaPub)).accept(new Init0(formulaPub.freeVariablesInOrder()));
        testHarnessPredPrev = new OneInputStreamOperatorTestHarness<>(new StreamFlatMap<>(statefulFlatMapFunctionPredPrev));
        testHarnessPredPrev.open();
        Either<String, GenFormula<VariableID>> c = Policy.read("PREVIOUS [0,7d] publish(160)");
        GenFormula<VariableID> prevFormula = c.right().get();
        FlatMapFunction<PipelineEvent, PipelineEvent> statefulFlatMapFunctionPrev = (MPrev) (convert(prevFormula)).accept(new Init0(prevFormula.freeVariablesInOrder()));
        testHarnessPrev = new OneInputStreamOperatorTestHarness<>(new StreamFlatMap<>(statefulFlatMapFunctionPrev));
        testHarnessPrev.open();

        //testNext()
        Either<String, GenFormula<VariableID>> pubr = Policy.read("publish(r)");
        GenFormula<VariableID> pubR = pubr.right().get();
        FlatMapFunction<Fact, PipelineEvent> statefulFlatMapFunctionPredNext = (MPred) (convert(pubR)).accept(new Init0(pubR.freeVariablesInOrder()));
        testHarnessPredNext = new OneInputStreamOperatorTestHarness<>(new StreamFlatMap<>(statefulFlatMapFunctionPredNext));
        testHarnessPredNext.open();
        Either<String, GenFormula<VariableID>> d = Policy.read("NEXT [0,7d] publish(163)");
        GenFormula<VariableID> nextFormula = d.right().get();
        FlatMapFunction<PipelineEvent, PipelineEvent> statefulFlatMapFunctionNext = (MNext) (convert(nextFormula)).accept(new Init0(nextFormula.freeVariablesInOrder()));
        testHarnessNext = new OneInputStreamOperatorTestHarness<>(new StreamFlatMap<>(statefulFlatMapFunctionNext));
        testHarnessNext.open();

        //testAnd()
        Either<String, GenFormula<VariableID>> andF = Policy.read("approve(r) AND publish(r)");
        GenFormula<VariableID> andFormula = andF.right().get();
        CoFlatMapFunction<PipelineEvent, PipelineEvent, PipelineEvent> statefulFlatMapFunctionAnd = (MAnd) (convert(andFormula)).accept(new Init0(andFormula.freeVariablesInOrder()));
        testHarnessAnd = new TwoInputStreamOperatorTestHarness<>(new CoStreamFlatMap<>(statefulFlatMapFunctionAnd));
        testHarnessAnd.open();
        Either<String, GenFormula<VariableID>> f1 = Policy.read("publish(r)");
        GenFormula<VariableID> formula1 = f1.right().get();
        FlatMapFunction<Fact, PipelineEvent> statefulFlatMapFunctionPred1 = (MPred) (convert(formula1)).accept(new Init0(andFormula.freeVariablesInOrder()));
        testHarnessPred1 = new OneInputStreamOperatorTestHarness<>(new StreamFlatMap<>(statefulFlatMapFunctionPred1));
        testHarnessPred1.open();
        Either<String, GenFormula<VariableID>> f2 = Policy.read("approve(r)");
        GenFormula<VariableID> formula2 = f2.right().get();
        FlatMapFunction<Fact, PipelineEvent> statefulFlatMapFunctionPred2 = (MPred) (convert(formula2)).accept(new Init0(andFormula.freeVariablesInOrder()));
        testHarnessPred2 = new OneInputStreamOperatorTestHarness<>(new StreamFlatMap<>(statefulFlatMapFunctionPred2));
        testHarnessPred2.open();


        //testOr()
        Either<String, GenFormula<VariableID>> f1Or = Policy.read("publish(r)");
        GenFormula<VariableID> formula1Or = f1Or.right().get();
        FlatMapFunction<Fact, PipelineEvent> statefulFlatMapFunctionPred1Or = (MPred) (convert(formula1Or)).accept(new Init0(formula1Or.freeVariablesInOrder()));
        testHarnessPred1Or = new OneInputStreamOperatorTestHarness<>(new StreamFlatMap<>(statefulFlatMapFunctionPred1Or));
        testHarnessPred1Or.open();
        Either<String, GenFormula<VariableID>> f2Or = Policy.read("approve(r)");
        GenFormula<VariableID> formula2Or = f2Or.right().get();
        FlatMapFunction<Fact, PipelineEvent> statefulFlatMapFunctionPred2Or = (MPred) (convert(formula2Or)).accept(new Init0(formula2Or.freeVariablesInOrder()));
        testHarnessPred2Or = new OneInputStreamOperatorTestHarness<>(new StreamFlatMap<>(statefulFlatMapFunctionPred2Or));
        testHarnessPred2Or.open();
        Either<String, GenFormula<VariableID>> orF = Policy.read("approve(r) OR publish(r)");
        GenFormula<VariableID> orFormula = orF.right().get();
        CoFlatMapFunction<PipelineEvent, PipelineEvent, PipelineEvent> statefulFlatMapFunctionOr = (MOr) (convert(orFormula)).accept(new Init0(orFormula.freeVariablesInOrder()));
        testHarnessOr = new TwoInputStreamOperatorTestHarness<>(new CoStreamFlatMap<>(statefulFlatMapFunctionOr));
        testHarnessOr.open();

        //testExists()
        Either<String, GenFormula<VariableID>> f1Ex = Policy.read("publish(x)");
        GenFormula<VariableID> formula1Ex = f1Ex.right().get();
        FlatMapFunction<Fact, PipelineEvent> statefulFlatMapFunctionPredExists = (MPred) (convert(formula1Ex)).accept(new Init0(formula1Ex.freeVariablesInOrder()));
        testHarnessPredExists = new OneInputStreamOperatorTestHarness<>(new StreamFlatMap<>(statefulFlatMapFunctionPredExists));
        testHarnessPredExists.open();
        Either<String, GenFormula<VariableID>> exForm = Policy.read("EXISTS x. publish(x)");
        GenFormula<VariableID> exFormula = exForm.right().get();
        FlatMapFunction<PipelineEvent, PipelineEvent> statefulFlatMapFunctionExists = (MExists) (convert(exFormula)).accept(new Init0(exFormula.freeVariablesInOrder()));
        testHarnessExists = new OneInputStreamOperatorTestHarness<>(new StreamFlatMap<>(statefulFlatMapFunctionExists));
        testHarnessExists.open();

        //testSince()
        Either<String, GenFormula<VariableID>> f1Since = Policy.read("publish(163)");
        GenFormula<VariableID> formula1Since = f1Since.right().get();
        FlatMapFunction<Fact, PipelineEvent> statefulFlatMapFunctionPred1Since = (MPred) (convert(formula1Since)).accept(new Init0(formula1Since.freeVariablesInOrder()));
        testHarnessPred1Since = new OneInputStreamOperatorTestHarness<>(new StreamFlatMap<>(statefulFlatMapFunctionPred1Since));
        testHarnessPred1Since.open();
        Either<String, GenFormula<VariableID>> f2Since = Policy.read("approve(163)");
        GenFormula<VariableID> formula2Since = f2Since.right().get();
        FlatMapFunction<Fact, PipelineEvent> statefulFlatMapFunctionPred2Since = (MPred) (convert(formula2Since)).accept(new Init0(formula2Since.freeVariablesInOrder()));
        testHarnessPred2Since = new OneInputStreamOperatorTestHarness<>(new StreamFlatMap<>(statefulFlatMapFunctionPred2Since));
        testHarnessPred2Since.open();
        Either<String, GenFormula<VariableID>> sinceF = Policy.read("publish(163) SINCE [0,7d] approve(163)");
        GenFormula<VariableID> sinceFormula = sinceF.right().get();
        CoFlatMapFunction<PipelineEvent, PipelineEvent, PipelineEvent> statefulFlatMapFunctionSince = (MSince) (convert(sinceFormula)).accept(new Init0(sinceFormula.freeVariablesInOrder()));
        testHarnessSince = new TwoInputStreamOperatorTestHarness<>(new CoStreamFlatMap<>(statefulFlatMapFunctionSince));
        testHarnessSince.open();

        //testUntil()
        Either<String, GenFormula<VariableID>> f1Until = Policy.read("publish(163)");
        GenFormula<VariableID> formula1Until = f1Until.right().get();
        FlatMapFunction<Fact, PipelineEvent> statefulFlatMapFunctionPred1Until = (MPred) (convert(formula1Until)).accept(new Init0(formula1Until.freeVariablesInOrder()));
        testHarnessPred1Until = new OneInputStreamOperatorTestHarness<>(new StreamFlatMap<>(statefulFlatMapFunctionPred1Until));
        testHarnessPred1Until.open();
        Either<String, GenFormula<VariableID>> f2Until = Policy.read("approve(163)");
        GenFormula<VariableID> formula2Until = f2Until.right().get();
        FlatMapFunction<Fact, PipelineEvent> statefulFlatMapFunctionPred2Until = (MPred) (convert(formula2Until)).accept(new Init0(formula2Until.freeVariablesInOrder()));
        testHarnessPred2Until = new OneInputStreamOperatorTestHarness<>(new StreamFlatMap<>(statefulFlatMapFunctionPred2Until));
        testHarnessPred2Until.open();
        Either<String, GenFormula<VariableID>> untilF = Policy.read("publish(163) UNTIL [0,7d] approve(163)");
        GenFormula<VariableID> untilFormula = untilF.right().get();
        CoFlatMapFunction<PipelineEvent, PipelineEvent, PipelineEvent> statefulFlatMapFunctionUntil = (MUntil) (convert(untilFormula)).accept(new Init0(untilFormula.freeVariablesInOrder()));
        testHarnessUntil = new TwoInputStreamOperatorTestHarness<>(new CoStreamFlatMap<>(statefulFlatMapFunctionUntil));
        testHarnessUntil.open();

        //testOnce()
        Either<String, GenFormula<VariableID>> predOnce = Policy.read("publish(163)");
        GenFormula<VariableID> formulaPredOnce = predOnce.right().get();
        FlatMapFunction<Fact, PipelineEvent> statefulFlatMapFunctionPredOnce = (MPred) (convert(formulaPredOnce)).accept(new Init0(formulaPredOnce.freeVariablesInOrder()));
        testHarnessPredOnce = new OneInputStreamOperatorTestHarness<>(new StreamFlatMap<>(statefulFlatMapFunctionPredOnce));
        testHarnessPredOnce.open();
        Either<String, GenFormula<VariableID>> onceEitherFormula = Policy.read("ONCE [0,7d] publish(163)");
        GenFormula<VariableID> onceFormula = onceEitherFormula.right().get();
        FlatMapFunction<PipelineEvent, PipelineEvent> statefulFlatMapFunctionOnce = (MOnce) (convert(onceFormula)).accept(new Init0(onceFormula.freeVariablesInOrder()));
        testHarnessOnce = new OneInputStreamOperatorTestHarness<>(new StreamFlatMap<>(statefulFlatMapFunctionOnce));
        testHarnessOnce.open();


        //testEventually()
        Either<String, GenFormula<VariableID>> predEv = Policy.read("publish(163)");
        GenFormula<VariableID> formulaPredEv = predEv.right().get();
        FlatMapFunction<Fact, PipelineEvent> statefulFlatMapFunctionPredEv = (MPred) (convert(formulaPredEv)).accept(new Init0(formulaPredEv.freeVariablesInOrder()));
        testHarnessPredEv = new OneInputStreamOperatorTestHarness<>(new StreamFlatMap<>(statefulFlatMapFunctionPredEv));
        testHarnessPredEv.open();
        Either<String, GenFormula<VariableID>> evEitherFormula = Policy.read("EVENTUALLY [0,7d] publish(163)");
        GenFormula<VariableID> evFormula = evEitherFormula.right().get();
        FlatMapFunction<PipelineEvent, PipelineEvent> statefulFlatMapFunctionEv = (MEventually) (convert(evFormula)).accept(new Init0(evFormula.freeVariablesInOrder()));
        testHarnessEv = new OneInputStreamOperatorTestHarness<>(new StreamFlatMap<>(statefulFlatMapFunctionEv));
        testHarnessEv.open();
    }

    @Test
    public void testUntil() throws Exception{
        //Persisting issue: datastructures should be cleared at the end to avoid memory leaks
        //Persisting issues: I don't "start from startEvalTimepoint"
        //formula under test: publish(163) UNTIL [0,7d] approve(163)
        testHarnessPred1Until.processElement(Fact.makeTP("publish", 1308477599,2L, "152"), 1L);
        testHarnessPred1Until.processElement(Fact.makeTP("publish", 1307955600,1L, "160"), 1L);
        testHarnessPred1Until.processElement(Fact.makeTP("publish", 1307955600,1L, "163"), 1L);
        testHarnessPred1Until.processElement(Fact.makeTP(null, 1307955600,1L, "163"), 1L);
        testHarnessPred1Until.processElement(Fact.makeTP(null, 1307532861,0L, "152"), 1L);
        testHarnessPred1Until.processElement(Fact.makeTP(null, 1308477599,2L, "152"), 1L);
        testHarnessPred1Until.processElement(Fact.makeTP(null, 1408477599,3L, "152"), 1L);
        //////////////////////////////////////////////////////////////////////////////////////////////////
        testHarnessPred2Until.processElement(Fact.makeTP("approve", 1307532861,0L, "152"), 1L);
        testHarnessPred2Until.processElement(Fact.makeTP("approve", 1308477599,2L, "163"), 1L);
        testHarnessPred2Until.processElement(Fact.makeTP("approve", 1308477599,2L, "187"), 1L);
        testHarnessPred2Until.processElement(Fact.makeTP(null, 1307955600,1L, "163"), 1L);
        testHarnessPred2Until.processElement(Fact.makeTP(null, 1307532861,0L, "152"), 1L);
        testHarnessPred2Until.processElement(Fact.makeTP(null, 1308477599,2L, "152"), 1L);
        testHarnessPred2Until.processElement(Fact.makeTP(null, 1408477599,3L, "152"), 1L);
        List<PipelineEvent> pes1 = testHarnessPred1Until.getOutput().stream().map(x -> (PipelineEvent)((StreamRecord) x).getValue()).collect(Collectors.toList());
        List<PipelineEvent> pes2 = testHarnessPred2Until.getOutput().stream().map(x -> (PipelineEvent)((StreamRecord) x).getValue()).collect(Collectors.toList());
        int longer = Math.max(pes1.size(), pes2.size());
        int shorter = Math.min(pes1.size(), pes2.size());
        for(int i = 0; i < longer; i++){
            if(i < shorter){
                testHarnessUntil.processElement1(pes1.get(i), 1L);
                testHarnessUntil.processElement2(pes2.get(i), 1L);
            }else if(pes1.size()==longer){
                testHarnessUntil.processElement1(pes1.get(i), 1L);
            }else{
                testHarnessUntil.processElement2(pes2.get(i), 1L);
            }
        }
        List<PipelineEvent> processedUntil = testHarnessUntil.getOutput().stream().map(x -> (PipelineEvent)((StreamRecord) x).getValue()).collect(Collectors.toList());
        System.out.println("testUntil() output:  " + processedUntil.toString());
        ArrayList<PipelineEvent> expectedResults = new ArrayList<>(Arrays.asList(
                PipelineEvent.terminator(1307532861, 0L),
                PipelineEvent.event(1307955600, 1L, Assignment.one()),
                PipelineEvent.terminator(1307955600, 1L),
                PipelineEvent.event(1308477599, 2L, Assignment.one()),
                PipelineEvent.terminator(1308477599, 2L)
        ));
        assertArrayEquals( expectedResults.toArray(), processedUntil.toArray());
    }

    @Test
    public void testSince() throws Exception{
        //Persisting issue: datastructures should be cleared at the end to avoid memory leaks
        testHarnessPred1Since.processElement(Fact.makeTP(null, 1307532861,0L, "152"), 1L);
        testHarnessPred1Since.processElement(Fact.makeTP("publish", 1307955600,1L, "160"), 1L);
        testHarnessPred1Since.processElement(Fact.makeTP(null, 1307955600,1L, "163"), 1L);
        testHarnessPred1Since.processElement(Fact.makeTP("publish", 1308477599,2L, "163"), 1L);
        testHarnessPred1Since.processElement(Fact.makeTP("publish", 1308477599,2L, "152"), 1L);
        testHarnessPred1Since.processElement(Fact.makeTP(null, 1308477599,2L, "152"), 1L);
        //////////////////////////////////////////////////////////////////////////////////////////////////
        testHarnessPred2Since.processElement(Fact.makeTP("approve", 1307532861,0L, "152"), 1L);
        testHarnessPred2Since.processElement(Fact.makeTP(null, 1307532861,0L, "152"), 1L);
        testHarnessPred2Since.processElement(Fact.makeTP("approve", 1307955600,1L, "163"), 1L);
        testHarnessPred2Since.processElement(Fact.makeTP(null, 1307955600,1L, "163"), 1L);
        testHarnessPred2Since.processElement(Fact.makeTP("approve", 1308477599,2L, "187"), 1L);
        testHarnessPred2Since.processElement(Fact.makeTP(null, 1308477599,2L, "152"), 1L);
        List<PipelineEvent> pes1 = testHarnessPred1Since.getOutput().stream().map(x -> (PipelineEvent)((StreamRecord) x).getValue()).collect(Collectors.toList());
        List<PipelineEvent> pes2 = testHarnessPred2Since.getOutput().stream().map(x -> (PipelineEvent)((StreamRecord) x).getValue()).collect(Collectors.toList());
        int longer = Math.max(pes1.size(), pes2.size());
        int shorter = Math.min(pes1.size(), pes2.size());
        for(int i = 0; i < longer; i++){
            if(i < shorter){
                testHarnessSince.processElement1(pes1.get(i), 1L);
                testHarnessSince.processElement2(pes2.get(i), 1L);
            }else if(pes1.size()==longer){
                testHarnessSince.processElement1(pes1.get(i), 1L);
            }else{
                testHarnessSince.processElement2(pes2.get(i), 1L);
            }
        }
        List<PipelineEvent> processedSince = testHarnessSince.getOutput().stream().map(x -> (PipelineEvent)((StreamRecord) x).getValue()).collect(Collectors.toList());
        System.out.println("testSince() output:  " + processedSince.toString());
        //formula being tested against: publish(163) SINCE [0,7d] approve(163)
        //approve(152) does not even satisfy the predicates, so it should not reach the binary operator for Since
        ArrayList<PipelineEvent> expectedResults = new ArrayList<>(Arrays.asList(
                PipelineEvent.terminator(1307532861, 0L),
                PipelineEvent.event(1307955600, 1L,  Assignment.one()),
                PipelineEvent.terminator(1307955600, 1L),
                PipelineEvent.event(1308477599, 2L,  Assignment.one()),
                PipelineEvent.terminator(1308477599, 2L)
        ));
        assertArrayEquals( expectedResults.toArray(), processedSince.toArray());
    }

    @Test
    public void testExists() throws Exception{
        //Persisting issue: Not sure if this test makes sense!
        testHarnessPredExists.processElement(Fact.makeTP(null, 1307532861,0L, "152"), 1L);
        testHarnessPredExists.processElement(Fact.makeTP("publish", 1307955600,1L, "160"), 1L);
        testHarnessPredExists.processElement(Fact.makeTP("publish", 1307955600,1L, "163"), 1L);
        testHarnessPredExists.processElement(Fact.makeTP(null, 1307955600,1L, "163"), 1L);
        testHarnessPredExists.processElement(Fact.makeTP("publish", 1308477599,2L, "163"), 1L);
        testHarnessPredExists.processElement(Fact.makeTP("publish", 1308477599,2L, "152"), 1L);
        testHarnessPredExists.processElement(Fact.makeTP(null, 1308477599,2L, "152"), 1L);
        /////////////////////////////////////////////////////////////////////////////////////////////////////
        List<PipelineEvent> pes1 = testHarnessPredExists.getOutput().stream().map(x -> (PipelineEvent)((StreamRecord) x).getValue()).collect(Collectors.toList());
        for (PipelineEvent pipelineEvent : pes1) {
            testHarnessExists.processElement(pipelineEvent, 1L);
        }
        List<PipelineEvent> processedExists = testHarnessExists.getOutput().stream().map(x -> (PipelineEvent)((StreamRecord) x).getValue()).collect(Collectors.toList());
        System.out.println("testExists() output:  " + processedExists.toString());
        ArrayList<PipelineEvent> expectedResults = new ArrayList<>(Arrays.asList(
                PipelineEvent.terminator(1307532861, 0L),
                PipelineEvent.event(1307955600, 1L,  Assignment.one()),
                PipelineEvent.event(1307955600, 1L,  Assignment.one()),
                PipelineEvent.terminator(1307955600, 1L),
                PipelineEvent.event(1308477599, 2L,  Assignment.one()),
                PipelineEvent.event(1308477599, 2L,  Assignment.one()),
                PipelineEvent.terminator(1308477599, 2L)
        ));
        assertArrayEquals(expectedResults.toArray(), processedExists.toArray());
    }

    @Test
    public void testAnd2() throws Exception{

        List<PipelineEvent> p1 = Arrays.asList(
                PipelineEvent.terminator(1307532861, 0L),
                PipelineEvent.event(1307955600, 1L, Assignment.one(Optional.of(160))),
                PipelineEvent.event(1307955600, 1L, Assignment.one(Optional.of(163))),
                PipelineEvent.terminator(1307955600, 1L),
                PipelineEvent.event(1308477599, 2L, Assignment.one(Optional.of(163))),
                PipelineEvent.event(1308477599, 2L, Assignment.one(Optional.of(152))),
                PipelineEvent.terminator(1308477599, 2L));

        List<PipelineEvent> p2 = Arrays.asList(
                PipelineEvent.event(1307532861, 0L,Assignment.one(Optional.of(152))),
                PipelineEvent.terminator(1307532861, 0L),
                PipelineEvent.event(1307955600, 1L, Assignment.one(Optional.of(163))),
                PipelineEvent.terminator(1307955600, 1L),
                PipelineEvent.event(1308477599, 2L, Assignment.one(Optional.of(187))),
                PipelineEvent.terminator(1308477599, 2L));


        int longer = Math.max(p1.size(), p2.size());
        int shorter = Math.min(p1.size(), p2.size());
        for(int i = 0; i < longer; i++){
            if(i < shorter){
                testHarnessAnd.processElement1(p1.get(i), 1L);
                testHarnessAnd.processElement2(p2.get(i), 1L);
            }else if(p1.size()==longer){
                testHarnessAnd.processElement1(p1.get(i), 1L);
            }else{
                testHarnessAnd.processElement2(p2.get(i), 1L);
            }
        }
        List<PipelineEvent> processedAnd = testHarnessAnd.getOutput().stream().map(x -> (PipelineEvent)((StreamRecord) x).getValue()).collect(Collectors.toList());
        System.out.println("test output:  " + processedAnd.toString());
        List<PipelineEvent> expectedResults = Arrays.asList(
                PipelineEvent.terminator(1307532861, 0L),
                PipelineEvent.event(1307955600, 1L, Assignment.one(Optional.of(163))),
                PipelineEvent.terminator(1307955600, 1L),
                PipelineEvent.terminator(1308477599, 2L)
        );
        assertArrayEquals(expectedResults.toArray(), processedAnd.toArray());
    }

    @Test
    public void testOr() throws Exception{
        // TODO explain well why it is that the events that are processed by each operator (in this case MOr) release things out-of-order
        testHarnessPred1Or.processElement(Fact.makeTP(null, 1307532861,0L, "152"), 1L);

        testHarnessPred1Or.processElement(Fact.makeTP("publish", 1307955600,1L, "160"), 1L);
        testHarnessPred1Or.processElement(Fact.makeTP("publish", 1307955600,1L, "163"), 1L);
        testHarnessPred1Or.processElement(Fact.makeTP(null, 1307955600,1L, "163"), 1L);

        testHarnessPred1Or.processElement(Fact.makeTP("publish", 1308477599,2L, "163"), 1L);
        testHarnessPred1Or.processElement(Fact.makeTP("publish", 1308477599,2L, "152"), 1L);
        testHarnessPred1Or.processElement(Fact.makeTP(null, 1308477599,2L, "152"), 1L);
        //////////////////////////////////////////////////////////////////////////////////////////////////
        testHarnessPred2Or.processElement(Fact.makeTP("approve", 1307532861,0L, "152"), 1L);
        testHarnessPred2Or.processElement(Fact.makeTP(null, 1307532861,0L, "152"), 1L);

        testHarnessPred2Or.processElement(Fact.makeTP("approve", 1307955600,1L, "163"), 1L);
        testHarnessPred2Or.processElement(Fact.makeTP(null, 1307955600,1L, "163"), 1L);

        testHarnessPred2Or.processElement(Fact.makeTP("approve", 1308477599,2L, "187"), 1L);
        testHarnessPred2Or.processElement(Fact.makeTP(null, 1308477599,2L, "152"), 1L);

        List<PipelineEvent> pes1 = testHarnessPred1Or.getOutput().stream().map(x -> (PipelineEvent)((StreamRecord) x).getValue()).collect(Collectors.toList());
        List<PipelineEvent> pes2 = testHarnessPred2Or.getOutput().stream().map(x -> (PipelineEvent)((StreamRecord) x).getValue()).collect(Collectors.toList());

        int longer = Math.max(pes1.size(), pes2.size());
        int shorter = Math.min(pes1.size(), pes2.size());
        for(int i = 0; i < longer; i++){
            if(i < shorter){
                testHarnessOr.processElement1(pes1.get(i), 1L);
                testHarnessOr.processElement2(pes2.get(i), 1L);
            }else if(pes1.size()==longer){
                testHarnessOr.processElement1(pes1.get(i), 1L);
            }else{
                testHarnessOr.processElement2(pes2.get(i), 1L);
            }
        }
        List<PipelineEvent> processedOr = testHarnessOr.getOutput().stream().map(x -> (PipelineEvent)((StreamRecord) x).getValue()).collect(Collectors.toList());
        System.out.println("test output:  " + processedOr.toString());
        ArrayList<PipelineEvent> expectedResults = new ArrayList<>(Arrays.asList(
                PipelineEvent.event(1307532861, 0L,  Assignment.one(Optional.of(152))),
                PipelineEvent.event(1307955600, 1L, Assignment.one(Optional.of(160))),
                PipelineEvent.terminator(1307532861, 0L),
                //Problem: there is a duplicate between the below two satisfactions
                PipelineEvent.event(1307955600, 1L,  Assignment.one(Optional.of(163))),
                PipelineEvent.event(1307955600, 1L,  Assignment.one(Optional.of(163))),
                PipelineEvent.terminator(1307955600, 1L),
                PipelineEvent.event(1308477599, 2L, Assignment.one(Optional.of(163))),
                PipelineEvent.event(1308477599, 2L,  Assignment.one(Optional.of(187))),
                PipelineEvent.event(1308477599, 2L,  Assignment.one(Optional.of(152))),
                PipelineEvent.terminator(1308477599, 2L)
        ));
        assertArrayEquals( expectedResults.toArray(), processedOr.toArray());
    }



    @Test
    public void testNext() throws Exception{
        //try a test where you remove below line
        testHarnessPredNext.processElement(Fact.makeTP("publish", 1307532861,0L, "159"), 1L);
        testHarnessPredNext.processElement(Fact.makeTP(null, 1307532861,0L, "152"), 1L);
        testHarnessPredNext.processElement(Fact.makeTP("publish", 1307955600,1L, "160"), 1L);
        testHarnessPredNext.processElement(Fact.makeTP(null, 1307955600,1L, "160"), 1L);
        testHarnessPredNext.processElement(Fact.makeTP("publish", 1308477599,2L, "163"), 1L);
        testHarnessPredNext.processElement(Fact.makeTP("publish", 1308477599,2L, "152"), 1L);
        testHarnessPredNext.processElement(Fact.makeTP(null, 1308477599,2L, "152"), 1L);
        //for the last 3 facts, we cannot check the interval condition, because we don't have the timestamp of the next timepoint!'
        List<PipelineEvent> pes = testHarnessPredNext.getOutput().stream().map(x -> (PipelineEvent)((StreamRecord) x).getValue()).collect(Collectors.toList());
        for (PipelineEvent pe : pes) {
            testHarnessNext.processElement(pe, 1L);
        }
        List<PipelineEvent> processedPES = testHarnessNext.getOutput().stream().map(x -> (PipelineEvent)((StreamRecord) x).getValue()).collect(Collectors.toList());
        System.out.println("testNext() output:  " + processedPES.toString());
        ArrayList<PipelineEvent> expectedResults = new ArrayList<>(Arrays.asList(
                PipelineEvent.event(1307532861, 0L,  Assignment.one(Optional.of(160))),
                PipelineEvent.terminator(1307532861, 0L),
                PipelineEvent.event(1307955600, 1L, Assignment.one(Optional.of(163))),
                PipelineEvent.event(1307955600, 1L, Assignment.one(Optional.of(152))),
                PipelineEvent.terminator(1307955600, 1L)
        ));
        assertArrayEquals(expectedResults.toArray(), processedPES.toArray());
    }



    @Test
    public void firstExample() throws Exception{
        testHarnessPred.processElement(Fact.makeTP(null, 1307532861,0L, "152"), 1L);
        testHarnessPred.processElement(Fact.makeTP("publish", 1307955600,1L, "160"), 1L);
        testHarnessPred.processElement(Fact.makeTP("publish", 1307955600,1L, "163"), 1L);
        testHarnessPred.processElement(Fact.makeTP(null, 1307955600,1L, "163"), 1L);
        testHarnessPred.processElement(Fact.makeTP("publish", 1308477599,2L, "163"), 1L);
        testHarnessPred.processElement(Fact.makeTP("publish", 1308477599,2L, "152"), 1L);
        testHarnessPred.processElement(Fact.makeTP(null, 1308477599,2L, "152"), 1L);

        List<PipelineEvent> pes = testHarnessPred.getOutput().stream().map(x -> (PipelineEvent)((StreamRecord) x).getValue()).collect(Collectors.toList());
        ArrayList<PipelineEvent> expectedResults = new ArrayList<>(Arrays.asList(
                PipelineEvent.terminator(1307532861, 0L),
                PipelineEvent.event(1307955600, 1L,  Assignment.one(Optional.of(160))),
                PipelineEvent.event(1307955600, 1L, Assignment.one(Optional.of(163))),
                PipelineEvent.terminator(1307955600, 1L),
                PipelineEvent.event(1308477599, 2L, Assignment.one(Optional.of(163))),
                PipelineEvent.event(1308477599, 2L, Assignment.one(Optional.of(152))),
                PipelineEvent.terminator(1308477599, 2L)
        ));
        assertArrayEquals(expectedResults.toArray(), pes.toArray());
        //assertEquals(expectedResults, pes);
        //Arrays.asList returns a List implementation, but it's not a java.util.ArrayList, which is what we need for
        //comparison with pes.
    }


    @Test
    public void testPred() throws Exception{
        testHarnessPred.processElement(Fact.makeTP("publish", 1L,0L, "160"), 1L);
        List<PipelineEvent> pe = testHarnessPred.getOutput().stream().map(x -> (PipelineEvent)((StreamRecord) x).getValue()).collect(Collectors.toList());
        assert(pe.size()==1);
        assert(pe.get(0).get().size() == 1);
        assert(pe.get(0).getTimestamp() == 1L);
        assert(pe.get(0).getTimepoint() == 0L);
        assert(pe.get(0).get().get(0).isPresent());
        assert(pe.get(0).get().get(0).get().equals("160"));
    }

    @Test
    public void testParsingWithFlink() throws Exception{
        testHarness.processElement("@0 p() q() @1 r(1,3) s(\"foo\") @2", 1L);
        //the test harness operator simulates an operator
        //test correct facts
        List<Fact> facts = testHarness.getOutput().stream().map(x -> (Fact)((StreamRecord) x).getValue()).collect(Collectors.toList());
        assertEquals(Arrays.asList(
                Fact.make("p", 0L),
                Fact.make("q", 0L),
                Fact.terminator(0L),
                Fact.make("r", 1L, "1","3"),
                Fact.make("s", 1L, "foo"),
                Fact.terminator(1L)
        ), facts);
        //test timepoints
        facts.forEach(f -> {assertEquals(f.getTimepoint(),f.getTimestamp());});
    }

    @Test
    public void testAndFreeVariables() throws Exception{

        testHarnessPred1.processElement(Fact.makeTP("publish", 1307955600,1L, "1"), 1L);
        testHarnessPred1.processElement(Fact.makeTP("publish", 1307955600,1L, "2"), 1L);
        testHarnessPred1.processElement(Fact.makeTP(null, 1307955600,1L, "163"), 1L);

        testHarnessPred2.processElement(Fact.makeTP("approve", 1307955600,1L, "3"), 1L);
        testHarnessPred2.processElement(Fact.makeTP(null, 1307955600,1L, "163"), 1L);

        List<PipelineEvent> pes1 = testHarnessPred1.getOutput().stream().map(x -> (PipelineEvent)((StreamRecord) x).getValue()).collect(Collectors.toList());
        List<PipelineEvent> pes2 = testHarnessPred2.getOutput().stream().map(x -> (PipelineEvent)((StreamRecord) x).getValue()).collect(Collectors.toList());

        int longer = Math.max(pes1.size(), pes2.size());
        int shorter = Math.min(pes1.size(), pes2.size());
        for(int i = 0; i < longer; i++){
            if(i < shorter){
                testHarnessAnd.processElement1(pes1.get(i), 1L);
                testHarnessAnd.processElement2(pes2.get(i), 1L);
            }else if(pes1.size()==longer){
                testHarnessAnd.processElement1(pes1.get(i), 1L);
            }else{
                testHarnessAnd.processElement2(pes2.get(i), 1L);
            }
        }
        List<PipelineEvent> processedAnd = testHarnessAnd.getOutput().stream().map(x -> (PipelineEvent)((StreamRecord) x).getValue()).collect(Collectors.toList());
        System.out.println("test output:  " + processedAnd.toString());
        ArrayList<PipelineEvent> expectedResults = new ArrayList<>(Arrays.asList(
                PipelineEvent.event(1307955600, 1L, Assignment.nones(2)),
                PipelineEvent.event(1307955600, 1L, Assignment.nones(2)),
                PipelineEvent.terminator(1307955600, 1L)));
        assertArrayEquals(expectedResults.toArray(), processedAnd.toArray());
    }


}
