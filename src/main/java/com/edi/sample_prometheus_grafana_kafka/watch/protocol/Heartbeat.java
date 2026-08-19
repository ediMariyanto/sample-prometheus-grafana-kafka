package com.edi.sample_prometheus_grafana_kafka.watch.protocol;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/** Periodic liveness frame. Everything the fleet view renders comes from here. */
public record Heartbeat(
        String agentId,
        String agentVersion,
        String appVersion,
        Long uptimeSeconds,
        Boolean mainappConnected,
        String mainappStatus,
        Instant mainappLastSeen,
        List<MainAppInfo> mainapps,
        Map<String, Object> metrics
) {
}
