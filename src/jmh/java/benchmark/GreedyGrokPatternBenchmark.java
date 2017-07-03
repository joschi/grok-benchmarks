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

public class GreedyGrokPatternBenchmark {
    @State(Scope.Thread)
    public static class BenchmarkState {
        private static final Map<String, String> PATTERNS = new HashMap<>();
        private static final String SEARCH_PATTERN = "%{GREEDYDATA:data} %{NUMBER:my_number}";
        static final String SEARCH_TEXT;

        static {
            PATTERNS.put("GREEDYDATA", ".*");
            PATTERNS.put("BASE10NUM", "(?<![0-9.+-])(?>[+-]?(?:(?:[0-9]+(?:\\.[0-9]+)?)|(?:\\.[0-9]+)))");
            PATTERNS.put("NUMBER", "(?:%{BASE10NUM})");

            final int size = 16184;
            final StringBuilder sb = new StringBuilder(size + 3);
            for (int i = 0; i < size; i++) {
                sb.append('a');
            }
            sb.append(" 42");
            SEARCH_TEXT = sb.toString();
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
    public void testElasticsearchGrokProcessorGreedyMatch(BenchmarkState state, Blackhole bh) throws Exception {
        final GrokProcessor grokProcessor = state.getGrokProcessor();

        final IngestDocument ingestDocument = new IngestDocument("index", "type", "0", null, null, null, null, Collections.singletonMap("field", BenchmarkState.SEARCH_TEXT));
        grokProcessor.execute(ingestDocument);

        bh.consume(ingestDocument);
    }

    @Benchmark
    public void testTheKrakkenGrokGreedyMatch(BenchmarkState state, Blackhole bh) {
        final Grok grok = state.getGrok();
        final Match match = grok.match(BenchmarkState.SEARCH_TEXT);
        match.captures();

        bh.consume(match);
    }

}
