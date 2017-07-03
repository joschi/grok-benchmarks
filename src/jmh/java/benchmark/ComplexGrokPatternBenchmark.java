package benchmark;

import io.thekraken.grok.api.Grok;
import io.thekraken.grok.api.Match;
import io.thekraken.grok.api.exception.GrokException;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.common.GrokProcessor;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ComplexGrokPatternBenchmark {

    // Example data taken from: https://github.com/elastic/elasticsearch/blob/v5.4.2/modules/ingest-common/src/test/java/org/elasticsearch/ingest/common/GrokTests.java#L241-L302
    @State(Scope.Thread)
    public static class BenchmarkState {
        private static final Map<String, String> PATTERNS = new HashMap<>();
        private static final String SEARCH_PATTERN = "%{IPORHOST:clientip} %{USER:ident} %{USER:auth} \\[%{HTTPDATE:timestamp}\\] \"%{WORD:verb} %{DATA:request} " +
                "HTTP/%{NUMBER:httpversion}\" %{NUMBER:response:int} (?:-|%{NUMBER:bytes:int}) %{QS:referrer} %{QS:agent}";
        static final String TEXT = "83.149.9.216 - - [19/Jul/2015:08:13:42 +0000] \"GET /presentations/logstash-monitorama-2013/images/" +
                "kibana-dashboard3.png HTTP/1.1\" 200 171717 \"http://semicomplete.com/presentations/logstash-monitorama-2013/\" " +
                "\"Mozilla" +
                "/5.0 (Macintosh; Intel Mac OS X 10_9_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/32.0.1700.77 Safari/537.36\"";

        static {
            PATTERNS.put("MONTHDAY", "(?:(?:0[1-9])|(?:[12][0-9])|(?:3[01])|[1-9])");
            PATTERNS.put("MONTH", "\\b(?:Jan(?:uary|uar)?|Feb(?:ruary|ruar)?|M(?:a|ä)?r(?:ch|z)?|Apr(?:il)?|Ma(?:y|i)?|Jun(?:e|i)" +
                    "?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|O(?:c|k)?t(?:ober)?|Nov(?:ember)?|De(?:c|z)(?:ember)?)\\b");
            PATTERNS.put("MINUTE", "(?:[0-5][0-9])");
            PATTERNS.put("YEAR", "(?>\\d\\d){1,2}");
            PATTERNS.put("HOUR", "(?:2[0123]|[01]?[0-9])");
            PATTERNS.put("SECOND", "(?:(?:[0-5]?[0-9]|60)(?:[:.,][0-9]+)?)");
            PATTERNS.put("TIME", "(?!<[0-9])%{HOUR}:%{MINUTE}(?::%{SECOND})(?![0-9])");
            PATTERNS.put("INT", "(?:[+-]?(?:[0-9]+))");
            PATTERNS.put("HTTPDATE", "%{MONTHDAY}/%{MONTH}/%{YEAR}:%{TIME} %{INT}");
            PATTERNS.put("WORD", "\\b\\w+\\b");
            PATTERNS.put("BASE10NUM", "(?<![0-9.+-])(?>[+-]?(?:(?:[0-9]+(?:\\.[0-9]+)?)|(?:\\.[0-9]+)))");
            PATTERNS.put("NUMBER", "(?:%{BASE10NUM})");
            PATTERNS.put("IPV6", "((([0-9A-Fa-f]{1,4}:){7}([0-9A-Fa-f]{1,4}|:))|(([0-9A-Fa-f]{1,4}:){6}(:[0-9A-Fa-f]{1,4}|((25[0-5]|2[0-4]" +
                    "\\d|1\\d\\d|[1-9]?\\d)(\\.(25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)){3})|:))|(([0-9A-Fa-f]{1,4}:){5}(((:[0-9A-Fa-f]{1,4})" +
                    "{1,2})|:((25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)(\\.(25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)){3})|:))|(([0-9A-Fa-f]{1,4}:)" +
                    "{4}(((:[0-9A-Fa-f]{1,4}){1,3})|((:[0-9A-Fa-f]{1,4})?:((25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)(\\.(25[0-5]|2[0-4]\\d|1\\" +
                    "d\\d|[1-9]?\\d)){3}))|:))|(([0-9A-Fa-f]{1,4}:){3}(((:[0-9A-Fa-f]{1,4}){1,4})|((:[0-9A-Fa-f]{1,4}){0,2}:((25[0-5]|2[0-4]" +
                    "\\d|1\\d\\d|[1-9]?\\d)(\\.(25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)){3}))|:))|(([0-9A-Fa-f]{1,4}:){2}(((:[0-9A-Fa-f]{1,4})" +
                    "{1,5})" +
                    "|((:[0-9A-Fa-f]{1,4}){0,3}:((25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)(\\.(25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)){3}))|:))" +
                    "|(([0-9A-Fa-f]{1,4}:){1}(((:[0-9A-Fa-f]{1,4}){1,6})|((:[0-9A-Fa-f]{1,4}){0,4}:((25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)" +
                    "(\\.(25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)){3}))|:))|(:(((:[0-9A-Fa-f]{1,4}){1,7})|((:[0-9A-Fa-f]{1,4}){0,5}" +
                    ":((25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)(\\.(25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)){3}))|:)))(%.+)?");
            PATTERNS.put("IPV4", "(?<![0-9])(?:(?:[0-1]?[0-9]{1,2}|2[0-4][0-9]|25[0-5])[.](?:[0-1]?[0-9]{1,2}|2[0-4][0-9]|25[0-5])[.]" +
                    "(?:[0-1]?[0-9]{1,2}|2[0-4][0-9]|25[0-5])[.](?:[0-1]?[0-9]{1,2}|2[0-4][0-9]|25[0-5]))(?![0-9])");
            PATTERNS.put("IP", "(?:%{IPV6}|%{IPV4})");
            PATTERNS.put("HOSTNAME", "\\b(?:[0-9A-Za-z][0-9A-Za-z-]{0,62})(?:\\.(?:[0-9A-Za-z][0-9A-Za-z-]{0,62}))*(\\.?|\\b)");
            PATTERNS.put("IPORHOST", "(?:%{IP}|%{HOSTNAME})");
            PATTERNS.put("USER", "[a-zA-Z0-9._-]+");
            PATTERNS.put("DATA", ".*?");
            PATTERNS.put("QS", "(?>(?<!\\\\)(?>\"(?>\\\\.|[^\\\\\"]+)+\"|\"\"|(?>'(?>\\\\.|[^\\\\']+)+')|''|(?>`(?>\\\\.|[^\\\\`]+)+`)|``))");

        }

        GrokProcessor grokProcessor;
        Grok grok;

        @Setup(Level.Trial)
        public void doSetup() throws GrokException {
            grokProcessor = new GrokProcessor("", PATTERNS, Collections.singletonList(SEARCH_PATTERN), "field", false, true);

            grok = new Grok();
            PATTERNS.forEach((name, pattern) -> silentlyAddGrokPattern(grok, name, pattern));
            grok.compile(SEARCH_PATTERN);
        }

        private void silentlyAddGrokPattern(Grok grok, String name, String pattern) {
            try {
                grok.addPattern(name, pattern);
            } catch (GrokException e) {
                throw new IllegalArgumentException(e);
            }
        }

        GrokProcessor getGrokProcessor() {
            return grokProcessor;
        }

        Grok getGrok() {
            return grok;
        }
    }

    @Benchmark
    public void testElasticsearchGrokProcessorSimpleMatch(BenchmarkState state, Blackhole bh) throws Exception {
        final GrokProcessor grokProcessor = state.getGrokProcessor();

        final IngestDocument ingestDocument = new IngestDocument("index", "type", "0", null, null, null, null, Collections.singletonMap("field", BenchmarkState.TEXT));
        grokProcessor.execute(ingestDocument);

        bh.consume(ingestDocument);
    }

    @Benchmark
    public void testTheKrakkenGrokSimpleMatch(BenchmarkState state, Blackhole bh) {
        final Grok grok = state.getGrok();
        final Match match = grok.match(BenchmarkState.TEXT);
        match.captures();

        bh.consume(match);
    }
}
