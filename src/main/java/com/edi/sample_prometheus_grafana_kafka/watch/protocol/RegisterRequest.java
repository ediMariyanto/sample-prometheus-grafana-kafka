package com.edi.sample_prometheus_grafana_kafka.watch.protocol;

import java.util.Map;

/**
 * First frame after the socket opens. The hub keys everything off
 * {@code agentId} (must be a stable UUID), so we persist ours to disk.
 *
 * @param enrollToken one-shot token from the Enrollment page. Optional: a
 *                    brand-new agent id with no token is accepted in "grace
 *                    mode" and shows up as {@code unenrolled} in the UI.
 * @param credential  echoed back on reconnect — minted by the hub on the first
 *                    token-based enrollment and returned in the register_ack.
 */
public record RegisterRequest(
        String agentId,
        String hostname,
        String os,
        String agentVersion,
        String appVersion,
        String enrollToken,
        String credential,
        Map<String, String> metadata,
        HostInfo hostInfo
) {
}
