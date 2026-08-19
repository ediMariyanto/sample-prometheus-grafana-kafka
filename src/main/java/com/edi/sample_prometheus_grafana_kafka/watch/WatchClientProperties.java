package com.edi.sample_prometheus_grafana_kafka.watch;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

/** Everything the embedded Nayaga Watch agent needs, under {@code watch.client}. */
@ConfigurationProperties(prefix = "watch.client")
public class WatchClientProperties {

    /** Master switch. Off means the component is never created at all. */
    private boolean enabled = true;

    /** Agent WebSocket endpoint on client-hub. */
    private String hubUrl = "ws://localhost:4010/agent/ws";

    /**
     * Wire identity in the applications registry. Must match
     * {@code ^[a-z0-9][a-z0-9._-]{0,63}$} and stay stable across releases —
     * patches and deployments are keyed off it.
     */
    private String appId = "sample-prometheus-grafana-kafka";

    /** Display name used when the hub auto-creates the registry row. */
    private String appName = "Sample Prometheus Grafana Kafka";

    /** Falls back to the jar's build-info version when left empty. */
    private String appVersion;

    /** Reported as the agent's own version, kept separate from appVersion. */
    private String agentVersion = "embedded-1.0.0";

    /**
     * One-shot token from the Enrollment page. Leave empty to register in
     * grace mode (works, but the workstation shows as {@code unenrolled}).
     */
    private String enrollToken;

    /** Hub treats an agent as long_down after 300s, so stay well under that. */
    private Duration heartbeatInterval = Duration.ofSeconds(30);

    /** Where the agent UUID and hub-issued credential are persisted. */
    private String identityFile = ".watch/agent-identity.properties";

    /** Optional human labels shown in the fleet view. */
    private String location;
    private String assignedTo;

    /** Log files the hub may ask the agent to collect, keyed by label. */
    private Map<String, String> logPaths = new LinkedHashMap<>();

    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }

    public String getHubUrl() { return hubUrl; }
    public void setHubUrl(String hubUrl) { this.hubUrl = hubUrl; }

    public String getAppId() { return appId; }
    public void setAppId(String appId) { this.appId = appId; }

    public String getAppName() { return appName; }
    public void setAppName(String appName) { this.appName = appName; }

    public String getAppVersion() { return appVersion; }
    public void setAppVersion(String appVersion) { this.appVersion = appVersion; }

    public String getAgentVersion() { return agentVersion; }
    public void setAgentVersion(String agentVersion) { this.agentVersion = agentVersion; }

    public String getEnrollToken() { return enrollToken; }
    public void setEnrollToken(String enrollToken) { this.enrollToken = enrollToken; }

    public Duration getHeartbeatInterval() { return heartbeatInterval; }
    public void setHeartbeatInterval(Duration heartbeatInterval) { this.heartbeatInterval = heartbeatInterval; }

    public String getIdentityFile() { return identityFile; }
    public void setIdentityFile(String identityFile) { this.identityFile = identityFile; }

    public String getLocation() { return location; }
    public void setLocation(String location) { this.location = location; }

    public String getAssignedTo() { return assignedTo; }
    public void setAssignedTo(String assignedTo) { this.assignedTo = assignedTo; }

    public Map<String, String> getLogPaths() { return logPaths; }
    public void setLogPaths(Map<String, String> logPaths) { this.logPaths = logPaths; }
}
