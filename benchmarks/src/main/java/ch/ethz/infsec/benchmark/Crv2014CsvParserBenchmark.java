package ch.ethz.infsec.benchmark;

import ch.ethz.infsec.monitor.Fact;
import ch.ethz.infsec.trace.parser.Crv2014CsvParser;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.util.ArrayList;
import java.util.Random;

@State(Scope.Thread)
public class Crv2014CsvParserBenchmark {
    private Random random;
    private ArrayList<Fact> sink;
    private Crv2014CsvParser parser;

    @Setup
    public void setUp() {
        random = new Random();
        sink = new ArrayList<>();
        parser = new Crv2014CsvParser();
    }

    private static final String[] inputs = new String[]{
            "foo, tp=1234, ts=1554989406",
            "fact name, tp = 123456789, ts = 1554989407, " +
                    "attrib1 = 987654321, attrib2 = this is some long string, attrib3 = foo bar\n",
            "a rather long fact name, tp = 123456789, ts = 1554989408, " +
                    "x1 = another longish string ..., x2 = 2, x3 = 3, x4 = 4, x5 = 5, x6 = 6\r\n"
    };

    @Benchmark
    public ArrayList<Fact> parse() throws Exception {
        sink.clear();
        parser.parseLine(sink::add, inputs[random.nextInt(inputs.length)]);
        parser.endOfInput(sink::add);
        return sink;
    }
}
