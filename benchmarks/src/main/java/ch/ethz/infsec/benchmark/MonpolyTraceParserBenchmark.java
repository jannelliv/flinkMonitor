package ch.ethz.infsec.benchmark;

import ch.ethz.infsec.monitor.Fact;
import ch.ethz.infsec.trace.parser.MonpolyTraceParser;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.util.ArrayList;
import java.util.Random;

@State(Scope.Thread)
public class MonpolyTraceParserBenchmark {
    private Random random;
    private ArrayList<Fact> sink;
    private MonpolyTraceParser parser;

    @Setup
    public void setUp() {
        random = new Random();
        sink = new ArrayList<>();
        parser = new MonpolyTraceParser();
    }

    private static final String[] inputs = new String[]{
            "@1554989406 foo ()",
            "@1554989407 fact_name (987654321, \"this is some long string\", foo-bar)\n",
            "@1554989408 a_rather_long_fact_name (\"another longish string ...\", 2, 3, 4, 5, 6)\r\n"
    };

    @Benchmark
    public ArrayList<Fact> parse() throws Exception {
        sink.clear();
        parser.parse(sink::add, inputs[random.nextInt(inputs.length)]);
        parser.endOfInput(sink::add);
        return sink;
    }
}
