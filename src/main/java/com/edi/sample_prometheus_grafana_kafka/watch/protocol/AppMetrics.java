package com.edi.sample_prometheus_grafana_kafka.watch.protocol;

import java.time.Instant;

/** Per-process resource sample attached to each MainApp in a heartbeat. */
public record AppMetrics(Long rssBytes, Double cpuPercent, Integer numThreads, Instant sampledAt) {
}
