package com.edi.sample_prometheus_grafana_kafka.watch.protocol;

import java.util.Map;

/**
 * The supervised application reported in every heartbeat. The hub auto-creates
 * an {@code applications} row the first time it sees an unknown {@code appId} —
 * that row is what the /client-apps page lists, initially as
 * {@code unregistered} until an admin activates it.
 *
 * @param status one of unknown | alive | stale | dead
 */
public record MainAppInfo(
        String appId,
        String appName,
        String appVersion,
        String status,
        java.time.Instant lastSeen,
        String exitReason,
        Integer pid,
        AppMetrics metrics,
        String pinnedVersion,
        String installPath,
        String userId,
        String userDisplayName,
        Map<String, String> logPaths
) {
}
