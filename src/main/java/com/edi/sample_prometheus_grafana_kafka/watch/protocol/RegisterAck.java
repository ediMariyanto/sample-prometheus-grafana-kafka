package com.edi.sample_prometheus_grafana_kafka.watch.protocol;

import java.util.Map;

/**
 * Hub's answer to a register. On rejection the hub also closes the socket with
 * 1008, so {@code message} is the only clue about why — log it.
 *
 * {@code config} carries {@code assigned_credential} on first enrollment only.
 */
public record RegisterAck(boolean success, String assignedId, String message, Map<String, String> config) {
}
