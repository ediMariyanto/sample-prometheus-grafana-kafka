package com.edi.sample_prometheus_grafana_kafka.dropout.parser;

import com.edi.sample_prometheus_grafana_kafka.dropout.model.MessageType;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/** Counters collected by a single {@link DropOutRowReader#read} pass. */
public final class ParseStats {

    private final long[] countByMessageId = new long[MessageType.MAX_ID + 1];

    private long rows;
    private long bytes;
    private long materialized;
    private long skipped;
    private long unknown;
    private long malformed;
    private long elapsedNanos;

    void addBytes(int read) {
        bytes += read;
    }

    void recordRow(int messageId) {
        rows++;
        if (messageId >= 0 && messageId <= MessageType.MAX_ID) {
            countByMessageId[messageId]++;
        }
    }

    void recordMaterialized() {
        materialized++;
    }

    void recordSkipped() {
        skipped++;
    }

    void recordUnknown() {
        unknown++;
    }

    void recordMalformed() {
        malformed++;
    }

    void finish(long elapsedNanos) {
        this.elapsedNanos = elapsedNanos;
    }

    public long rows() {
        return rows;
    }

    public long bytes() {
        return bytes;
    }

    /** Rows that were bound to a message object. */
    public long materialized() {
        return materialized;
    }

    /** Rows recognised but deliberately not bound, because the handler did not want the type. */
    public long skipped() {
        return skipped;
    }

    public long unknown() {
        return unknown;
    }

    public long malformed() {
        return malformed;
    }

    public long elapsedMillis() {
        return TimeUnit.NANOSECONDS.toMillis(elapsedNanos);
    }

    public long rowsPerSecond() {
        return elapsedNanos == 0 ? 0 : rows * 1_000_000_000L / elapsedNanos;
    }

    public long count(MessageType type) {
        return countByMessageId[type.id()];
    }

    public Map<MessageType, Long> countsByType() {
        Map<MessageType, Long> counts = new EnumMap<>(MessageType.class);
        for (MessageType type : MessageType.values()) {
            long count = countByMessageId[type.id()];
            if (count > 0) {
                counts.put(type, count);
            }
        }
        return counts;
    }
}
