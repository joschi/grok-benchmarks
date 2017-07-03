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

public class SimpleGrokPatternBenchmark {

    @State(Scope.Thread)
    public static class BenchmarkState {
        private static final Map<String, String> PATTERNS = new HashMap<>();
        private static final String SEARCH_PATTERN = "%{DATA} %{NUMBER:my_number}";

        static {
            PATTERNS.put("DATA", ".*?");
            PATTERNS.put("BASE10NUM", "(?<![0-9.+-])(?>[+-]?(?:(?:[0-9]+(?:\\.[0-9]+)?)|(?:\\.[0-9]+)))");
            PATTERNS.put("NUMBER", "(?:%{BASE10NUM})");
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

        final IngestDocument ingestDocument = new IngestDocument("index", "type", "0", null, null, null, null, Collections.singletonMap("field", "Foobar 42"));
        grokProcessor.execute(ingestDocument);

        bh.consume(ingestDocument);
    }

    @Benchmark
    public void testTheKrakkenGrokSimpleMatch(BenchmarkState state, Blackhole bh) {
        final Grok grok = state.getGrok();
        final Match match = grok.match("Foobar 42");
        match.captures();

        bh.consume(match);
    }
}
