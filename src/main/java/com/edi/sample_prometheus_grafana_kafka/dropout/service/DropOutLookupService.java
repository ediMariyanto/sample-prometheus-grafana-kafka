package com.edi.sample_prometheus_grafana_kafka.dropout.service;

import com.edi.sample_prometheus_grafana_kafka.dropout.dto.LoadSummary;
import com.edi.sample_prometheus_grafana_kafka.dropout.dto.MessageQuery;
import com.edi.sample_prometheus_grafana_kafka.dropout.index.DropOutIndex;
import com.edi.sample_prometheus_grafana_kafka.dropout.index.DropOutIndexBuilder;
import com.edi.sample_prometheus_grafana_kafka.dropout.index.InstrumentSnapshot;
import com.edi.sample_prometheus_grafana_kafka.dropout.index.MessageFilter;
import com.edi.sample_prometheus_grafana_kafka.dropout.index.MessagePage;
import com.edi.sample_prometheus_grafana_kafka.dropout.index.MessageSchema;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.MarketValues;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.MessageType;
import com.edi.sample_prometheus_grafana_kafka.dropout.parser.DropOutRowReader;
import com.edi.sample_prometheus_grafana_kafka.dropout.parser.ParseStats;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.ToIntFunction;

/**
 * Parses a drop-copy file into a {@link DropOutIndex} and serves lookups and filters against the
 * most recently loaded one.
 *
 * <p>The index is replaced atomically, so readers always see a complete, self-consistent snapshot -
 * a query running while a new file is being parsed keeps answering from the old index until the new
 * one is fully built.
 */
@Service
public class DropOutLookupService {

    private static final Logger log = LoggerFactory.getLogger(DropOutLookupService.class);

    /** Sentinel added to the order-book filter when a symbol resolves to nothing. */
    private static final long UNRESOLVABLE_ORDER_BOOK = Long.MIN_VALUE;

    private final DropOutRowReader reader = new DropOutRowReader();
    private final AtomicReference<DropOutIndex> index = new AtomicReference<>();
    private final AtomicReference<LoadSummary> lastSummary = new AtomicReference<>();

    private final Timer loadTimer;
    private final Counter rowCounter;

    public DropOutLookupService(MeterRegistry registry) {
        this.loadTimer = Timer.builder("dropout.file.load")
                .description("Time spent parsing a drop-copy file into the lookup index")
                .register(registry);
        this.rowCounter = Counter.builder("dropout.rows.parsed")
                .description("Rows read from drop-copy files")
                .register(registry);
        Gauge.builder("dropout.index.order.books", index, ref -> sizeOf(ref, DropOutIndex::orderBookCount))
                .description("Order books held in the current index")
                .register(registry);
        Gauge.builder("dropout.index.assets", index, ref -> sizeOf(ref, DropOutIndex::assetCount))
                .description("Assets held in the current index")
                .register(registry);
        Gauge.builder("dropout.index.retained.rows", index, ref -> sizeOf(ref, DropOutLookupService::retainedRows))
                .description("Message rows retained for filtering")
                .register(registry);
    }

    private static double sizeOf(AtomicReference<DropOutIndex> ref, ToIntFunction<DropOutIndex> size) {
        DropOutIndex current = ref.get();
        return current == null ? 0d : size.applyAsInt(current);
    }

    private static int retainedRows(DropOutIndex current) {
        int total = 0;
        for (int count : current.retainedCounts().values()) {
            total += count;
        }
        return total;
    }

    /**
     * Reads {@code in} to completion and swaps in the resulting index. The stream is consumed
     * incrementally; the caller owns closing it.
     */
    public LoadSummary load(String filename, InputStream in) throws IOException {
        return load(filename, in, Set.of());
    }

    /**
     * Reads {@code in} and swaps in the resulting index, leaving out the types in {@code skip}.
     * Skipping parses faster and costs no memory, but skipped types are then absent from
     * {@link #query} - the returned summary names them so that is never a surprise.
     */
    public LoadSummary load(String filename, InputStream in, Set<MessageType> skip) throws IOException {
        Set<MessageType> skipped = skip == null ? Set.of() : skip;
        if (skipped.size() >= MessageType.values().length) {
            // Every structure skipped would parse fine and leave an index nothing can be asked of.
            throw new IllegalArgumentException("cannot skip all " + MessageType.values().length
                    + " message types - the index would hold no rows to query");
        }
        DropOutIndexBuilder builder = new DropOutIndexBuilder(skipped);
        Timer.Sample sample = Timer.start();
        ParseStats stats = reader.read(in, builder);
        DropOutIndex built = builder.build(stats);
        sample.stop(loadTimer);
        rowCounter.increment(stats.rows());

        index.set(built);
        LoadSummary summary = toSummary(filename, built, stats);
        lastSummary.set(summary);
        log.info("Loaded {}: {} rows ({} retained, {} skipped) in {} ms = {} rows/s",
                filename, stats.rows(), stats.materialized(), stats.skipped(),
                stats.elapsedMillis(), stats.rowsPerSecond());
        return summary;
    }

    /** The current index, or empty when no file has been loaded yet. */
    public Optional<DropOutIndex> index() {
        return Optional.ofNullable(index.get());
    }

    public Optional<LoadSummary> lastSummary() {
        return Optional.ofNullable(lastSummary.get());
    }

    public Optional<InstrumentSnapshot> lookup(String query) {
        return index().flatMap(current -> current.lookup(query));
    }

    /**
     * Filters the retained rows of one message type. {@code symbol} is resolved through the index
     * first, so a ticker expands to every order book of that instrument.
     *
     * @throws IllegalStateException when no file has been loaded
     */
    public MessagePage query(MessageType type, MessageQuery request) {
        return query(type, request, Map.of());
    }

    /**
     * As above, plus conditions on the structure own fields. Any entry of {@code fieldParams} whose
     * key is not a reserved parameter is resolved against that message type schema, so every field
     * of every structure is filterable by its own name; an unknown name is rejected rather than
     * ignored, because a silently dropped condition looks like a successful query.
     */
    public MessagePage query(MessageType type, MessageQuery request, Map<String, List<String>> fieldParams) {
        DropOutIndex current = index.get();
        if (current == null) {
            throw new IllegalStateException("no file loaded yet");
        }
        if (current.skippedTypes().contains(type)) {
            // Returning an empty page here would be indistinguishable from a file that genuinely
            // carries no rows of this type, which is the one thing the caller must not confuse.
            throw new IllegalStateException(type.name() + " (messageId " + type.id()
                    + ") was skipped when the file was loaded, so no rows of it were retained;"
                    + " re-upload without skip=" + type.name() + " to query it");
        }
        MessageFilter filter = toFilter(current, type, request, fieldParams);
        return current.query(type, filter, request.pageOrDefault(), request.sizeOrDefault());
    }

    private static MessageFilter toFilter(DropOutIndex current, MessageType type, MessageQuery request,
                                          Map<String, List<String>> fieldParams) {
        Set<Long> orderBookIds = new LinkedHashSet<>();
        if (request.orderBookId() != null) {
            orderBookIds.add(request.orderBookId());
        }
        if (request.symbol() != null && !request.symbol().isBlank()) {
            Set<Long> resolved = current.resolveOrderBookIds(request.symbol());
            if (resolved.isEmpty()) {
                // An unresolvable symbol must match nothing, never fall through to matching everything.
                orderBookIds.add(UNRESOLVABLE_ORDER_BOOK);
            } else {
                orderBookIds.addAll(resolved);
            }
        }
        MessageFilter.Builder builder = MessageFilter.builder()
                .orderBookIds(orderBookIds)
                .name(request.name())
                .isin(request.isin())
                .assetId(request.assetId())
                .orderId(request.orderId())
                .matchId(request.matchId())
                .transactionId(request.transactionId())
                .actorId(request.actorId())
                .participantId(request.participantId())
                .side(request.side())
                .priceBetween(request.minPrice(), request.maxPrice())
                .quantityBetween(request.minQuantity(), request.maxQuantity())
                .timestampBetween(request.fromNanos(), request.toNanos())
                .partitionId(request.partitionId())
                .seqnumBetween(request.minSeqnum(), request.maxSeqnum());

        MessageSchema schema = MessageSchema.of(type);
        for (Map.Entry<String, List<String>> entry : fieldParams.entrySet()) {
            String key = entry.getKey();
            if (MessageQuery.RESERVED_PARAMS.contains(key) || entry.getValue().isEmpty()) {
                continue;
            }
            // Repeating a parameter means "any of these", the same as a comma-separated list.
            builder.field(schema, key, String.join(",", entry.getValue()));
        }
        return builder.build();
    }

    private static LoadSummary toSummary(String filename, DropOutIndex built, ParseStats stats) {
        Map<String, Long> rowsByType = new LinkedHashMap<>();
        for (Map.Entry<MessageType, Long> entry : stats.countsByType().entrySet()) {
            rowsByType.put(entry.getKey().name(), entry.getValue());
        }
        List<String> skippedTypes = new ArrayList<>();
        for (MessageType type : built.skippedTypes()) {
            skippedTypes.add(type.name());
        }
        Instant businessDate = built.businessDate() == null ? null : MarketValues.toInstant(built.businessDate());
        return new LoadSummary(
                filename,
                stats.bytes(),
                stats.rows(),
                stats.materialized(),
                stats.skipped(),
                stats.unknown(),
                stats.malformed(),
                stats.elapsedMillis(),
                stats.rowsPerSecond(),
                businessDate == null ? null : businessDate.toString(),
                new LoadSummary.IndexCounts(
                        built.assetCount(),
                        built.orderBookCount(),
                        built.participantCount(),
                        built.actorCount(),
                        built.orderCount(),
                        built.tradeCount(),
                        built.rejects().size()),
                rowsByType,
                List.copyOf(skippedTypes));
    }
}
