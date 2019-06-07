package ch.ethz.infsec.replayer;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.Semaphore;

import static org.junit.Assert.assertEquals;

public class ReplayerIT {
    private Process process;
    private ArrayList<Pair<Integer, String>> output;
    private Semaphore doneReading;

    private int roundRelativeMillis(long first, long current) {
        // round to the nearest interval of 0.1 seconds (= 100 ms)
        return Math.round((current - first) / 100.0f) * 100;
    }

    @Before
    public void setUp() throws IOException {
        Path executablePath = Paths.get(System.getProperty("basedir")).getParent().resolve("replayer.sh");
        process = new ProcessBuilder(executablePath.toString(),
                "-a", "10", "-f", "monpoly", "-t", "1000", "-T", "TIME:", "-C", "CMD:")
                .redirectError(ProcessBuilder.Redirect.INHERIT)
                .start();

        output = new ArrayList<>();
        doneReading = new Semaphore(0);
        Thread readerThread = new Thread(() -> {
            final BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            try {
                long firstReadMillis = -1;
                while ((line = reader.readLine()) != null) {
                    final long readMillis = System.currentTimeMillis();
                    if (firstReadMillis < 0) {
                        firstReadMillis = readMillis;
                    }
                    if (line.startsWith("TIME:")) {
                        final String[] parts = line.split(":", 2);
                        try {
                            final long timestampMillis = Long.valueOf(parts[1]);
                            line = "TIME:" + roundRelativeMillis(firstReadMillis, timestampMillis);
                        } catch (Exception ignored) {
                        }
                    }
                    output.add(Pair.of(roundRelativeMillis(firstReadMillis, readMillis), line));
                }
            } catch (IOException ignored) {
            }
            doneReading.release();
        });
        readerThread.setDaemon(true);
        readerThread.start();
    }

    @After
    public void tearDown() throws InterruptedException {
        if (process != null && process.isAlive()) {
            process.destroyForcibly().waitFor();
        }
    }

    @Test(timeout = 5000)
    public void testReplayer() throws IOException, InterruptedException {
        final String input = "abc, tp=1, ts=1000, x=foo\n" +
                "abc, tp=1, ts=1000, x=bar\n" +
                "def, tp=2, ts=1000, x=1234\n" +
                "def, tp=3, ts=1020, x=5678\n" +
                "CMD:this is a command\n" +
                "xyz, tp=4, ts=1030, x=1, y=2\n" +
                "xyz, tp=4, ts=1030, x=3, y=4\n";
        final BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(process.getOutputStream()));
        writer.write(input);
        writer.close();

        doneReading.acquire();
        process.waitFor();

        assertEquals(0, process.exitValue());
        assertEquals(Arrays.asList(
                Pair.of(0, "TIME:0"),
                Pair.of(0, "@1000 abc(foo)(bar)"),
                Pair.of(0, "@1000 def(1234)"),
                Pair.of(1000, "TIME:1000"),
                Pair.of(2000, "TIME:2000"),
                Pair.of(2000, "CMD:this is a command"),
                Pair.of(2000, "@1020 def(5678)"),
                Pair.of(3000, "TIME:3000"),
                Pair.of(3000, "@1030 xyz(1,2)(3,4)"),
                Pair.of(3000, "TIME:3000")
        ), output);
    }
}