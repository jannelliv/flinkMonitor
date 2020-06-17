package ch.ethz.infsec.generator;


import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SimpleSignature implements Signature {

    private List<String> events;
    private Map<String,Integer> arities;

    private SimpleSignature(List<String> e, Map<String,Integer> a){
        this.events=e;
        this.arities=a;
    }

    public List<String> getEvents(){
        return events;
    }
    public int getArity(String e){
        return arities.get(e);
    }

    private static final Pattern specificationPattern =
            Pattern.compile("(([a-zA-Z0-9_-]+)\\(([a-zA-Z0-9_:-]+(?:,\\s*[a-zA-Z0-9:_-]+)*)?\\)\\s*)");
    private static final Pattern argumentDelimiter = Pattern.compile(",\\s*");


    public static SimpleSignature parse(String path) throws InvalidEventPatternException, IOException {
        List<String> lines = Files.readAllLines(Paths.get(path));
        List<String> events = new ArrayList<String>();
        Map<String,Integer> arities = new HashMap<String,Integer>();

        for (String pattern : lines) {
            final Matcher matcher = specificationPattern.matcher(pattern);
            while (matcher.regionStart() < pattern.length()) {
                if (!matcher.find() || matcher.group(2) == null) {
                    throw new InvalidEventPatternException("Syntax error in event pattern");
                }
                String event = matcher.group(2);
                events.add(event);
                if(matcher.group(3)!=null){
                    final String[] args = argumentDelimiter.split(matcher.group(3));
                    arities.put(event, Collections.unmodifiableList(Arrays.asList(args)).size());
                } else {
                    arities.put(event, 0);
                }
                matcher.region(matcher.end(), pattern.length());
            }

        }
        if(events.size()==0){
            System.err.println("[Warning] Empty signature provided. Using the default one: P1(int)");
            events.add("P1");
            arities.put("P1", 1);
        }

        return new SimpleSignature(events, arities);
    }

    private static String string_of(String [] l){
        String res = "[";
        for (String s : l) {
            res+=s;
            res+="; ";
        }
        res+="]";
        return res;
    }

}
