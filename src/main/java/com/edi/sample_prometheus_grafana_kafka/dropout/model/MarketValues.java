package com.edi.sample_prometheus_grafana_kafka.dropout.model;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.OptionalLong;

/**
 * Helpers for the raw wire conventions used by the drop-copy feed.
 *
 * <p>Two conventions matter when reading a row:
 * <ul>
 *   <li>{@link Long#MIN_VALUE} is the "no value" sentinel (an absent price, an unset quantity).
 *       It must never be treated as a real number - see {@link #isPresent(long)}.</li>
 *   <li>Timestamps are epoch <em>nanoseconds</em>, not millis.</li>
 * </ul>
 */
public final class MarketValues {

    /** Sentinel the feed uses for "field carries no value". */
    public static final long ABSENT = Long.MIN_VALUE;

    private MarketValues() {
    }

    public static boolean isPresent(long raw) {
        return raw != ABSENT;
    }

    public static OptionalLong optional(long raw) {
        return raw == ABSENT ? OptionalLong.empty() : OptionalLong.of(raw);
    }

    /** Converts an epoch-nanosecond field to an {@link Instant}, or {@code null} when absent or zero. */
    public static Instant toInstant(long epochNanos) {
        if (epochNanos == ABSENT || epochNanos == 0L) {
            return null;
        }
        return Instant.ofEpochSecond(Math.floorDiv(epochNanos, 1_000_000_000L),
                Math.floorMod(epochNanos, 1_000_000_000L));
    }

    /**
     * Applies an order book's {@code decimalsInPrice} / {@code decimalsInQuantity} scale to a raw
     * integer field. Returns {@code null} when the raw value is absent.
     */
    public static BigDecimal scaled(long raw, int decimals) {
        return raw == ABSENT ? null : BigDecimal.valueOf(raw, decimals);
    }
}
