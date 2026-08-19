package com.edi.sample_prometheus_grafana_kafka.dropout;

import com.edi.sample_prometheus_grafana_kafka.dropout.index.DropOutIndex;
import com.edi.sample_prometheus_grafana_kafka.dropout.index.DropOutIndexBuilder;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.MessageType;
import com.edi.sample_prometheus_grafana_kafka.dropout.parser.DropOutRowReader;
import com.edi.sample_prometheus_grafana_kafka.dropout.parser.ParseStats;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.EnumSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Measures the reader against a real file. Skipped unless a path is supplied:
 *
 * <pre>./gradlew test -Ddropout.file=/path/to/DROP-OUT.json</pre>
 */
@EnabledIfSystemProperty(named = "dropout.file", matches = ".+")
class DropOutRowReaderBenchmark {

    private static final int RUNS = 3;

    @Test
    void reportsThroughput() throws IOException {
        Path file = Path.of(System.getProperty("dropout.file"));
        assertThat(file).exists();

        System.out.printf("%nfile: %s (%.1f MiB)%n", file, Files.size(file) / 1048576.0);

        ParseStats indexed = null;
        for (int run = 1; run <= RUNS; run++) {
            indexed = measure("lookup only (skips transaction envelopes)", run, DropOutIndexBuilder.ENVELOPE_TYPES);
        }
        for (int run = 1; run <= RUNS; run++) {
            measure("full index (retains every row)", run, Set.of());
        }

        // Build a full index and report what retaining every row actually costs in heap.
        Runtime runtime = Runtime.getRuntime();
        long usedBefore = usedHeap(runtime);
        DropOutIndexBuilder builder = new DropOutIndexBuilder();
        ParseStats stats;
        try (InputStream in = Files.newInputStream(file)) {
            stats = new DropOutRowReader().read(in, builder);
        }
        DropOutIndex index = builder.build(stats);
        long retained = index.retainedCounts().values().stream().mapToLong(Integer::longValue).sum();
        System.out.printf("retained %,d rows, heap grew %.1f MiB (~%d bytes/row)%n",
                retained, (usedHeap(runtime) - usedBefore) / 1048576.0,
                retained == 0 ? 0 : (usedHeap(runtime) - usedBefore) / retained);
        System.out.printf("assets=%d orderBooks=%d participants=%d actors=%d orders=%d trades=%d%n",
                index.assetCount(), index.orderBookCount(), index.participantCount(),
                index.actorCount(), index.orderCount(), index.tradeCount());
        stats.countsByType().forEach((type, count) -> System.out.printf("  %-24s %,10d%n", type, count));

        assertThat(indexed).isNotNull();
        assertThat(indexed.malformed()).isZero();
        assertThat(indexed.unknown()).isZero();
        assertThat(index.orderBookCount()).isPositive();
    }

    /** Best-effort used-heap reading; a GC first so the number reflects live objects, not garbage. */
    private static long usedHeap(Runtime runtime) {
        System.gc();
        return runtime.totalMemory() - runtime.freeMemory();
    }

    private ParseStats measure(String label, int run, Set<MessageType> skipped) throws IOException {
        Path file = Path.of(System.getProperty("dropout.file"));
        DropOutIndexBuilder builder = new DropOutIndexBuilder(
                skipped.isEmpty() ? EnumSet.noneOf(MessageType.class) : EnumSet.copyOf(skipped));
        ParseStats stats;
        try (InputStream in = Files.newInputStream(file)) {
            stats = new DropOutRowReader().read(in, builder);
        }
        System.out.printf("%-38s run %d: %,d rows (%,d bound, %,d skipped) in %,d ms = %,d rows/s, %.1f MiB/s%n",
                label, run, stats.rows(), stats.materialized(), stats.skipped(),
                stats.elapsedMillis(), stats.rowsPerSecond(),
                stats.elapsedMillis() == 0 ? 0 : (stats.bytes() / 1048576.0) / (stats.elapsedMillis() / 1000.0));
        return stats;
    }
}
