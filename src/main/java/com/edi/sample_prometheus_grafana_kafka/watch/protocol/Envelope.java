package com.edi.sample_prometheus_grafana_kafka.watch.protocol;

import java.time.Instant;

/**
 * Outer frame for every message on the client-hub agent WebSocket.
 * Mirrors {@code dev.nayaga.watch.client.protocol.Envelope} on the hub side —
 * the hub serialises with SNAKE_CASE, so field names here must stay one-word.
 *
 * @param payload already-serialised JSON tree for the inner message
 */
public record Envelope<T>(String type, Instant timestamp, T payload) {
}
