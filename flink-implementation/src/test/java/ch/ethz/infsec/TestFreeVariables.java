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


public class TestFreeVariables {

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
    private OneInputStreamOperatorTestHarness<Fact, PipelineEvent> testHarnessPred1fv;
    private OneInputStreamOperatorTestHarness<Fact, PipelineEvent> testHarnessPred2fv;
    private TwoInputStreamOperatorTestHarness<PipelineEvent, PipelineEvent, PipelineEvent> testHarnessAnd;
    private TwoInputStreamOperatorTestHarness<PipelineEvent, PipelineEvent, PipelineEvent> testHarnessAndFV;


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
        Either<String, GenFormula<VariableID>> a = Policy.read("publish(160)");
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
        Either<String, GenFormula<VariableID>> c = Policy.read("PREVIOUS [0,7] publish(160)");
        GenFormula<VariableID> prevFormula = c.right().get();
        FlatMapFunction<PipelineEvent, PipelineEvent> statefulFlatMapFunctionPrev = (MPrev) (convert(prevFormula)).accept(new Init0(prevFormula.freeVariablesInOrder()));
        testHarnessPrev = new OneInputStreamOperatorTestHarness<>(new StreamFlatMap<>(statefulFlatMapFunctionPrev));
        testHarnessPrev.open();

        //testNext()
        Either<String, GenFormula<VariableID>> pubr = Policy.read("publish(160)");
        GenFormula<VariableID> pubR = pubr.right().get();
        FlatMapFunction<Fact, PipelineEvent> statefulFlatMapFunctionPredNext = (MPred) (convert(pubR)).accept(new Init0(pubR.freeVariablesInOrder()));
        testHarnessPredNext = new OneInputStreamOperatorTestHarness<>(new StreamFlatMap<>(statefulFlatMapFunctionPredNext));
        testHarnessPredNext.open();
        Either<String, GenFormula<VariableID>> d = Policy.read("NEXT [0,7d] publish(160)");
        GenFormula<VariableID> nextFormula = d.right().get();
        FlatMapFunction<PipelineEvent, PipelineEvent> statefulFlatMapFunctionNext = (MNext) (convert(nextFormula)).accept(new Init0(nextFormula.freeVariablesInOrder()));
        testHarnessNext = new OneInputStreamOperatorTestHarness<>(new StreamFlatMap<>(statefulFlatMapFunctionNext));
        testHarnessNext.open();

        //testAnd()
        Either<String, GenFormula<VariableID>> f1 = Policy.read("publish(r)");
        GenFormula<VariableID> formula1 = f1.right().get();
        FlatMapFunction<Fact, PipelineEvent> statefulFlatMapFunctionPred1 = (MPred) (convert(formula1)).accept(new Init0(formula1.freeVariablesInOrder()));
        testHarnessPred1 = new OneInputStreamOperatorTestHarness<>(new StreamFlatMap<>(statefulFlatMapFunctionPred1));
        testHarnessPred1.open();
        Either<String, GenFormula<VariableID>> f2 = Policy.read("approve(r)");
        GenFormula<VariableID> formula2 = f2.right().get();
        FlatMapFunction<Fact, PipelineEvent> statefulFlatMapFunctionPred2 = (MPred) (convert(formula2)).accept(new Init0(formula2.freeVariablesInOrder()));
        testHarnessPred2 = new OneInputStreamOperatorTestHarness<>(new StreamFlatMap<>(statefulFlatMapFunctionPred2));
        testHarnessPred2.open();
        Either<String, GenFormula<VariableID>> andF = Policy.read("approve(r) AND publish(r)");
        GenFormula<VariableID> andFormula = andF.right().get();
        CoFlatMapFunction<PipelineEvent, PipelineEvent, PipelineEvent> statefulFlatMapFunctionAnd = (MAnd) (convert(andFormula)).accept(new Init0(andFormula.freeVariablesInOrder()));
        testHarnessAnd = new TwoInputStreamOperatorTestHarness<>(new CoStreamFlatMap<>(statefulFlatMapFunctionAnd));
        testHarnessAnd.open();


        //testAndFreeVariables()   --> try approve(r) and publish(r)

        Either<String, GenFormula<VariableID>> andFfv = Policy.read("approve(q) AND publish(r)");
        GenFormula<VariableID> andFormulafv = andFfv.right().get();
        CoFlatMapFunction<PipelineEvent, PipelineEvent, PipelineEvent> statefulFlatMapFunctionAndfv = (MAnd) (convert(andFormulafv)).accept(new Init0(andFormulafv.freeVariablesInOrder()));
        testHarnessAndFV = new TwoInputStreamOperatorTestHarness<>(new CoStreamFlatMap<>(statefulFlatMapFunctionAndfv));
        testHarnessAndFV.open();
        Either<String, GenFormula<VariableID>> f1fv = Policy.read("publish(r)");
        GenFormula<VariableID> formula1fv = f1fv.right().get();
        FlatMapFunction<Fact, PipelineEvent> statefulFlatMapFunctionPred1fv = (MPred) (convert(formula1fv)).accept(new Init0(andFormulafv.freeVariablesInOrder()));
        testHarnessPred1fv = new OneInputStreamOperatorTestHarness<>(new StreamFlatMap<>(statefulFlatMapFunctionPred1fv));
        testHarnessPred1fv.open();
        Either<String, GenFormula<VariableID>> f2fv = Policy.read("approve(q)");
        GenFormula<VariableID> formula2fv = f2fv.right().get();
        FlatMapFunction<Fact, PipelineEvent> statefulFlatMapFunctionPred2fv = (MPred) (convert(formula2fv)).accept(new Init0(andFormulafv.freeVariablesInOrder()));
        testHarnessPred2fv = new OneInputStreamOperatorTestHarness<>(new StreamFlatMap<>(statefulFlatMapFunctionPred2fv));
        testHarnessPred2fv.open();



        //testOr()
        Either<String, GenFormula<VariableID>> orF = Policy.read("approve(r) OR publish(r)");
        GenFormula<VariableID> orFormula = orF.right().get();
        CoFlatMapFunction<PipelineEvent, PipelineEvent, PipelineEvent> statefulFlatMapFunctionOr = (MOr) (convert(orFormula)).accept(new Init0(orFormula.freeVariablesInOrder()));
        testHarnessOr = new TwoInputStreamOperatorTestHarness<>(new CoStreamFlatMap<>(statefulFlatMapFunctionOr));
        testHarnessOr.open();
        Either<String, GenFormula<VariableID>> f1Or = Policy.read("publish(r)");
        GenFormula<VariableID> formula1Or = f1Or.right().get();
        FlatMapFunction<Fact, PipelineEvent> statefulFlatMapFunctionPred1Or = (MPred) (convert(formula1Or)).accept(new Init0(orFormula.freeVariablesInOrder()));
        testHarnessPred1Or = new OneInputStreamOperatorTestHarness<>(new StreamFlatMap<>(statefulFlatMapFunctionPred1Or));
        testHarnessPred1Or.open();
        Either<String, GenFormula<VariableID>> f2Or = Policy.read("approve(r)");
        GenFormula<VariableID> formula2Or = f2Or.right().get();
        FlatMapFunction<Fact, PipelineEvent> statefulFlatMapFunctionPred2Or = (MPred) (convert(formula2Or)).accept(new Init0(orFormula.freeVariablesInOrder()));
        testHarnessPred2Or = new OneInputStreamOperatorTestHarness<>(new StreamFlatMap<>(statefulFlatMapFunctionPred2Or));
        testHarnessPred2Or.open();


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
        //TODO: check more extensively that cleanDatastructures() does not mess up the functionality of the algorithm
        //Persisting issue: datastructures should be cleared at the end to avoid memory leaks
        //Persisting issues: I don't "start from startEvalTimepoint"
        //formula being tested: publish(163) UNTIL [0,7d] approve(163)
        testHarnessPred1Until.processElement(Fact.makeTP(null, 1307532861,0L, "152"), 1L);
        testHarnessPred1Until.processElement(Fact.makeTP("publish", 1307955600,1L, "160"), 1L);
        testHarnessPred1Until.processElement(Fact.makeTP(null, 1307955600,1L, "163"), 1L);
        testHarnessPred1Until.processElement(Fact.makeTP("publish", 1308477599,2L, "163"), 1L);
        testHarnessPred1Until.processElement(Fact.makeTP("publish", 1308477599,2L, "152"), 1L);
        testHarnessPred1Until.processElement(Fact.makeTP(null, 1308477599,2L, "152"), 1L);
        //////////////////////////////////////////////////////////////////////////////////////////////////
        testHarnessPred2Until.processElement(Fact.makeTP("approve", 1307532861,0L, "152"), 1L);
        testHarnessPred2Until.processElement(Fact.makeTP(null, 1307532861,0L, "152"), 1L);
        testHarnessPred2Until.processElement(Fact.makeTP("approve", 1307955600,1L, "163"), 1L);
        testHarnessPred2Until.processElement(Fact.makeTP(null, 1307955600,1L, "163"), 1L);
        testHarnessPred2Until.processElement(Fact.makeTP("approve", 1308477599,2L, "187"), 1L);
        testHarnessPred2Until.processElement(Fact.makeTP(null, 1308477599,2L, "152"), 1L);
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
        //formula under test: publish(163) UNTIL [0,7d] approve(163)
        ArrayList<PipelineEvent> expectedResults = new ArrayList<>(Arrays.asList(
                PipelineEvent.terminator(1307532861, 0L),
                PipelineEvent.event(1307955600, 1L, Assignment.one(Optional.of(163))),
                PipelineEvent.terminator(1307955600, 1L),
                //new PipelineEvent(1308477599, 2L, false, Assignment.one(Optional.of(163))),
                //We will not consider the above event as we are only contemplating infinite traces.
                PipelineEvent.terminator(1308477599, 2L)
        ));
        assertArrayEquals( expectedResults.toArray(), processedUntil.toArray());
    }


    /*@Test
    public void testOrFV() throws Exception{
        testHarnessPred1Or.processElement(Fact.makeTP("publish", 1L,0L, "1"), 1L);
        testHarnessPred2Or.processElement(Fact.makeTP("approve", 1L,0L, "50"), 1L);
        testHarnessPred1Or.processElement(Fact.makeTP(null, 1L,0L, "1"), 1L);
        testHarnessPred2Or.processElement(Fact.makeTP(null, 1L,0L, "1"), 1L);
        testHarnessPred1Or.processElement(Fact.makeTP("publish", 1L,1L, "2"), 1L);
        testHarnessPred1Or.processElement(Fact.makeTP(null, 1L,1L, "2"), 1L);
        testHarnessPred2Or.processElement(Fact.makeTP(null, 1L,1L, "2"), 1L);
        testHarnessPred1Or.processElement(Fact.makeTP("publish", 1L,2L, "160"), 1L);
        testHarnessPred2Or.processElement(Fact.makeTP("approve", 1L,2L, "160"), 1L);
        testHarnessPred1Or.processElement(Fact.makeTP(null, 1L,2L, "160"), 1L);
        testHarnessPred2Or.processElement(Fact.makeTP(null, 1L,2L, "160"), 1L);
        testHarnessPred1Or.processElement(Fact.makeTP("publish", 1L,3L, "3"), 1L);
        testHarnessPred1Or.processElement(Fact.makeTP(null, 1L,3L, "3"), 1L);
        testHarnessPred2Or.processElement(Fact.makeTP(null, 1L,3L, "3"), 1L);

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
                PipelineEvent.event(1307955600, 1L,  Assignment.one(Optional.of(163))),
                PipelineEvent.event(1307955600, 1L,  Assignment.one(Optional.of(163))),
                PipelineEvent.terminator(1307955600, 1L),
                PipelineEvent.event(1308477599, 2L, Assignment.one(Optional.of(163))),
                PipelineEvent.event(1308477599, 2L,  Assignment.one(Optional.of(187))),
                PipelineEvent.event(1308477599, 2L,  Assignment.one(Optional.of(152))),
                PipelineEvent.terminator(1308477599, 2L)
        ));
        assertArrayEquals( expectedResults.toArray(), processedOr.toArray());
    }*/

    @Test
    public void testRelTrueFalse() throws Exception{
        testHarnessRel.processElement(Fact.makeTP(null, 1L, 0L, "163"), 1L);
        List<PipelineEvent> pe = testHarnessRel.getOutput().stream().map(x -> (PipelineEvent)((StreamRecord) x).getValue()).collect(Collectors.toList());
        assert(pe.size()==1);
        assert(!pe.get(0).isPresent());
        assert(pe.get(0).getTimestamp() == 1L);
        assert(pe.get(0).getTimepoint() == 0L);
        assert(pe.get(0).get()==null);
    }

    @Test
    public void testPred() throws Exception{
        testHarnessPred.processElement(Fact.makeTP("publish", 1L,0L, "1"), 1L);
        testHarnessPred.processElement(Fact.makeTP(null, 1L,0L, "1"), 1L);
        testHarnessPred.processElement(Fact.makeTP("publish", 1L,1L, "2"), 1L);
        testHarnessPred.processElement(Fact.makeTP(null, 1L,1L, "2"), 1L);
        testHarnessPred.processElement(Fact.makeTP("publish", 1L,2L, "160"), 1L);
        testHarnessPred.processElement(Fact.makeTP(null, 1L,2L, "160"), 1L);
        testHarnessPred.processElement(Fact.makeTP("publish", 1L,3L, "3"), 1L);
        testHarnessPred.processElement(Fact.makeTP(null, 1L,3L, "3"), 1L);
        List<PipelineEvent> pe = testHarnessPred.getOutput().stream().map(x -> (PipelineEvent)((StreamRecord) x).getValue()).collect(Collectors.toList());
        System.out.println("test output:  " + pe.toString());
        ArrayList<PipelineEvent> expectedResults = new ArrayList<>(Arrays.asList(
                PipelineEvent.terminator(1, 0L),
                PipelineEvent.terminator(1, 1L),
                PipelineEvent.event(1, 2L, Assignment.one()),
                PipelineEvent.terminator(1, 2L),
                PipelineEvent.terminator(1, 3L)));
        assertArrayEquals(expectedResults.toArray(), pe.toArray());
    }

    @Test
    public void testPrev() throws Exception{
        testHarnessPredPrev.processElement(Fact.makeTP("publish", 1L,0L, "1"), 1L);
        testHarnessPredPrev.processElement(Fact.makeTP(null, 1L,0L, "1"), 1L);
        testHarnessPredPrev.processElement(Fact.makeTP("publish", 1L,1L, "2"), 1L);
        testHarnessPredPrev.processElement(Fact.makeTP(null, 1L,1L, "2"), 1L);
        testHarnessPredPrev.processElement(Fact.makeTP("publish", 1L,2L, "160"), 1L);
        testHarnessPredPrev.processElement(Fact.makeTP(null, 1L,2L, "160"), 1L);
        testHarnessPredPrev.processElement(Fact.makeTP("publish", 1L,3L, "3"), 1L);
        testHarnessPredPrev.processElement(Fact.makeTP(null, 1L,3L, "3"), 1L);
        List<PipelineEvent> pes = testHarnessPredPrev.getOutput().stream().map(x -> (PipelineEvent)((StreamRecord) x).getValue()).collect(Collectors.toList());
        for(PipelineEvent pe : pes){
            testHarnessPrev.processElement(pe, 1L);
        }
        List<PipelineEvent> pesPrev = testHarnessPrev.getOutput().stream().map(x -> (PipelineEvent)((StreamRecord) x).getValue()).collect(Collectors.toList());

        System.out.println("testPrev output:  " + pesPrev.toString());
        ArrayList<PipelineEvent> expectedResults = new ArrayList<>(Arrays.asList(
                PipelineEvent.terminator(1, 0L),
                PipelineEvent.terminator(1, 1L),
                PipelineEvent.terminator(1, 2L),
                PipelineEvent.event(1, 3L, Assignment.one()),
                PipelineEvent.terminator(1, 3L)));
        assertArrayEquals(expectedResults.toArray(), pesPrev.toArray());
    }

    @Test
    public void testPrev2() throws Exception{
        testHarnessPredPrev.processElement(Fact.makeTP("publish", 1L,3L, "3"), 1L);
        testHarnessPredPrev.processElement(Fact.makeTP(null, 1L,3L, "3"), 1L);
        testHarnessPredPrev.processElement(Fact.makeTP("publish", 1L,0L, "1"), 1L);
        testHarnessPredPrev.processElement(Fact.makeTP(null, 1L,0L, "1"), 1L);
        testHarnessPredPrev.processElement(Fact.makeTP("publish", 1L,2L, "160"), 1L);
        testHarnessPredPrev.processElement(Fact.makeTP(null, 1L,2L, "160"), 1L);
        testHarnessPredPrev.processElement(Fact.makeTP("publish", 1L,1L, "2"), 1L);
        testHarnessPredPrev.processElement(Fact.makeTP(null, 1L,1L, "2"), 1L);

        List<PipelineEvent> pes = testHarnessPredPrev.getOutput().stream().map(x -> (PipelineEvent)((StreamRecord) x).getValue()).collect(Collectors.toList());
        for(PipelineEvent pe : pes){
            testHarnessPrev.processElement(pe, 1L);
        }
        List<PipelineEvent> pesPrev = testHarnessPrev.getOutput().stream().map(x -> (PipelineEvent)((StreamRecord) x).getValue()).collect(Collectors.toList());

        System.out.println("testPrev2 output:  " + pesPrev.toString());
        ArrayList<PipelineEvent> expectedResults = new ArrayList<>(Arrays.asList(
                PipelineEvent.event(1, 3L, Assignment.one()),
                PipelineEvent.terminator(1, 3L),
                PipelineEvent.terminator(1, 2L),
                PipelineEvent.terminator(1, 0L),
                PipelineEvent.terminator(1, 1L)));
        assertArrayEquals(expectedResults.toArray(), pesPrev.toArray());
    }


    @Test
    public void testPrev3() throws Exception{
        testHarnessPredPrev.processElement(Fact.makeTP(null, 7532861L,0L, "3"), 1L);
        testHarnessPredPrev.processElement(Fact.makeTP("publish", 7955600L,1L, "160"), 1L);
        testHarnessPredPrev.processElement(Fact.makeTP("publish", 7955600L,1L, "163"), 1L);
        testHarnessPredPrev.processElement(Fact.makeTP(null, 7955600L,1L, "163"), 1L);
        testHarnessPredPrev.processElement(Fact.makeTP("publish", 8477599L,2L, "163"), 1L);
        testHarnessPredPrev.processElement(Fact.makeTP("publish", 8477599L,2L, "152"), 1L);
        testHarnessPredPrev.processElement(Fact.makeTP(null, 8477599L,2L, "163"), 1L);
        testHarnessPredPrev.processElement(Fact.makeTP(null, 8478000L,3L, "3"), 1L);
        List<PipelineEvent> pes = testHarnessPredPrev.getOutput().stream().map(x -> (PipelineEvent)((StreamRecord) x).getValue()).collect(Collectors.toList());
        for(PipelineEvent pe : pes){
            testHarnessPrev.processElement(pe, 1L);
        }
        List<PipelineEvent> pesPrev = testHarnessPrev.getOutput().stream().map(x -> (PipelineEvent)((StreamRecord) x).getValue()).collect(Collectors.toList());

        System.out.println("testPrev3 output:  " + pesPrev.toString());
        ArrayList<PipelineEvent> expectedResults = new ArrayList<>(Arrays.asList(
                PipelineEvent.terminator(7532861, 0L),
                PipelineEvent.terminator(7955600, 1L),
                PipelineEvent.terminator(8477599, 2L),
                PipelineEvent.terminator(8478000, 3L)));
        assertArrayEquals(expectedResults.toArray(), pesPrev.toArray());
    }

    @Test
    public void testNext() throws Exception{
        testHarnessPredNext.processElement(Fact.makeTP("publish", 1L,0L, "1"), 1L);
        testHarnessPredNext.processElement(Fact.makeTP(null, 1L,0L, "1"), 1L);
        testHarnessPredNext.processElement(Fact.makeTP("publish", 1L,1L, "2"), 1L);
        testHarnessPredNext.processElement(Fact.makeTP(null, 1L,1L, "2"), 1L);
        testHarnessPredNext.processElement(Fact.makeTP("publish", 1L,2L, "160"), 1L);
        testHarnessPredNext.processElement(Fact.makeTP(null, 1L,2L, "160"), 1L);
        testHarnessPredNext.processElement(Fact.makeTP("publish", 1L,3L, "3"), 1L);
        testHarnessPredNext.processElement(Fact.makeTP(null, 1L,3L, "3"), 1L);
        List<PipelineEvent> pes = testHarnessPredNext.getOutput().stream().map(x -> (PipelineEvent)((StreamRecord) x).getValue()).collect(Collectors.toList());
        for(PipelineEvent pe : pes){
            testHarnessNext.processElement(pe, 1L);
        }
        List<PipelineEvent> pesPrev = testHarnessNext.getOutput().stream().map(x -> (PipelineEvent)((StreamRecord) x).getValue()).collect(Collectors.toList());

        System.out.println("testNext output:  " + pesPrev.toString());
        ArrayList<PipelineEvent> expectedResults = new ArrayList<>(Arrays.asList(
                PipelineEvent.terminator(1, 0L),
                PipelineEvent.event(1, 1L, Assignment.one()),
                PipelineEvent.terminator(1, 1L),
                PipelineEvent.terminator(1, 2L)));
                //PipelineEvent.terminator(1, 3L)));
        assertArrayEquals(expectedResults.toArray(), pesPrev.toArray());
    }

    @Test
    public void testPred2() throws Exception{
        //formula: publish(163)
        testHarnessPredConst.processElement(Fact.makeTP("publish", 1L,0L, "163"), 1L);
        List<PipelineEvent> pe = testHarnessPredConst.getOutput().stream().map(x -> (PipelineEvent)((StreamRecord) x).getValue()).collect(Collectors.toList());
        assert(pe.size()==1);
        assert(pe.get(0).get().size() == 0);
        assert(pe.get(0).getTimestamp() == 1L);
        assert(pe.get(0).getTimepoint() == 0L);
    }



}
