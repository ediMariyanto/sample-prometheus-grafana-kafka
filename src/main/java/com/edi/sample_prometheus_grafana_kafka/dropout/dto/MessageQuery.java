package com.edi.sample_prometheus_grafana_kafka.dropout.dto;

import java.time.DateTimeException;
import java.time.Instant;

/**
 * Query-string binding for the message filter endpoint. Every field is optional; absent fields drop
 * out of the filter rather than matching nothing.
 *
 * <p>{@code from} / {@code to} accept either raw epoch nanoseconds (as the feed carries them) or an
 * ISO-8601 instant such as {@code 2026-05-05T03:15:00Z}, which is what a human actually has.
 */
public record MessageQuery(
        Long orderBookId,
        String symbol,
        String name,
        String isin,
        Long assetId,
        Long orderId,
        Long matchId,
        Long transactionId,
        Long actorId,
        Long participantId,
        Integer side,
        Long minPrice,
        Long maxPrice,
        Long minQuantity,
        Long maxQuantity,
        String from,
        String to,
        Integer partitionId,
        Long minSeqnum,
        Long maxSeqnum,
        Integer page,
        Integer size
) {

    public static final int DEFAULT_SIZE = 100;
    public static final int MAX_SIZE = 1000;

    /**
     * Parameter names this record already claims. Anything else on the query string is treated as a
     * condition on one of the structure's own fields, so these names win where they overlap - a
     * {@code name} or {@code side} parameter uses the cross-type mapping below rather than the raw
     * field, which is the same answer but works uniformly across structures.
     */
    public static final java.util.Set<String> RESERVED_PARAMS = java.util.Set.of(
            "orderBookId", "symbol", "name", "isin", "assetId", "orderId", "matchId", "transactionId",
            "actorId", "participantId", "side", "minPrice", "maxPrice", "minQuantity", "maxQuantity",
            "from", "to", "partitionId", "minSeqnum", "maxSeqnum", "page", "size");

    public int pageOrDefault() {
        return page == null || page < 0 ? 0 : page;
    }

    public int sizeOrDefault() {
        if (size == null || size <= 0) {
            return DEFAULT_SIZE;
        }
        return Math.min(size, MAX_SIZE);
    }

    public Long fromNanos() {
        return toEpochNanos(from, "from");
    }

    public Long toNanos() {
        return toEpochNanos(to, "to");
    }

    private static Long toEpochNanos(String value, String field) {
        if (value == null || value.isBlank()) {
            return null;
        }
        String trimmed = value.trim();
        if (isDigits(trimmed)) {
            try {
                return Long.valueOf(trimmed);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(field + " is not a valid epoch-nanosecond value: " + value, e);
            }
        }
        try {
            Instant instant = Instant.parse(trimmed);
            return Math.addExact(Math.multiplyExact(instant.getEpochSecond(), 1_000_000_000L), instant.getNano());
        } catch (DateTimeException | ArithmeticException e) {
            throw new IllegalArgumentException(
                    field + " must be epoch nanoseconds or an ISO-8601 instant, was: " + value, e);
        }
    }

    private static boolean isDigits(String value) {
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c < '0' || c > '9') {
                return false;
            }
        }
        return !value.isEmpty();
    }
}
