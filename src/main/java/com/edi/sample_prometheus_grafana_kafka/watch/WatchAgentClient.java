package com.edi.sample_prometheus_grafana_kafka.watch;

import com.edi.sample_prometheus_grafana_kafka.watch.protocol.AppMetrics;
import com.edi.sample_prometheus_grafana_kafka.watch.protocol.Envelope;
import com.edi.sample_prometheus_grafana_kafka.watch.protocol.Heartbeat;
import com.edi.sample_prometheus_grafana_kafka.watch.protocol.HostInfo;
import com.edi.sample_prometheus_grafana_kafka.watch.protocol.MainAppInfo;
import com.edi.sample_prometheus_grafana_kafka.watch.protocol.RegisterAck;
import com.edi.sample_prometheus_grafana_kafka.watch.protocol.RegisterRequest;
import com.fasterxml.jackson.annotation.JsonInclude;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.boot.info.BuildProperties;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.PropertyNamingStrategies;
import tools.jackson.databind.cfg.DateTimeFeature;
import tools.jackson.databind.json.JsonMapper;

/**
 * Embedded Nayaga Watch agent.
 *
 * The hub fleet protocol normally comes from the standalone Go agent that
 * supervises a separate MainApp process. Here the application supervises
 * itself: one WebSocket to {@code /agent/ws}, a {@code register} on connect and
 * a {@code heartbeat} on a timer, with this JVM reported as the single MainApp.
 * That heartbeat is what makes the app_id appear on the hub Applications page
 * (initially as {@code unregistered} — an admin still has to activate it).
 *
 * Failure to reach the hub is never fatal: the tick loop just retries, so the
 * business endpoints keep serving whether or not monitoring is up.
 */
@Component
@ConditionalOnProperty(prefix = "watch.client", name = "enabled", havingValue = "true", matchIfMissing = true)
class WatchAgentClient {

    private static final Logger log = LoggerFactory.getLogger(WatchAgentClient.class);

    /**
     * Hub serialises SNAKE_CASE with ISO-8601 timestamps; match it exactly.
     * Deliberately a private mapper — the app's own MVC mapper stays camelCase.
     */
    private final ObjectMapper mapper = JsonMapper.builder()
            .propertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
            .changeDefaultPropertyInclusion(v -> v.withValueInclusion(JsonInclude.Include.NON_NULL))
            .disable(DateTimeFeature.WRITE_DATES_AS_TIMESTAMPS)
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .build();

    private final WatchClientProperties props;
    private final AgentIdentityStore identity;
    private final UUID agentId;
    private final String appVersion;

    private final HttpClient http = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .build();

    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "watch-agent");
                t.setDaemon(true);
                return t;
            });

    /** Both written from the WS callback thread, read from the tick thread. */
    private volatile WebSocket socket;
    private volatile boolean registered;

    private long lastCpuSampleAt;
    private long lastCpuTimeNanos;

    WatchAgentClient(WatchClientProperties props, ObjectProvider<BuildProperties> buildInfo) {
        this.props = props;
        this.identity = new AgentIdentityStore(props.getIdentityFile());
        this.agentId = identity.agentId();
        this.appVersion = resolveAppVersion(props, buildInfo);
    }

    private static String resolveAppVersion(WatchClientProperties props, ObjectProvider<BuildProperties> buildInfo) {
        if (props.getAppVersion() != null && !props.getAppVersion().isBlank()) {
            return props.getAppVersion();
        }
        BuildProperties build = buildInfo.getIfAvailable();
        return build != null ? build.getVersion() : "unknown";
    }

    // -- Lifecycle --

    /**
     * Started only once the app is actually serving. Registering earlier would
     * advertise us as alive while the web layer is still coming up.
     */
    @EventListener(ApplicationReadyEvent.class)
    void start() {
        long periodMs = Math.max(1000L, props.getHeartbeatInterval().toMillis());
        log.info("watch: agent {} reporting app_id={} to {} every {}ms",
                agentId, props.getAppId(), props.getHubUrl(), periodMs);
        // A single fixed-delay loop covers both jobs: reconnect when down,
        // heartbeat when up. The heartbeat period doubles as the retry backoff,
        // which keeps a dead hub from being hammered.
        scheduler.scheduleWithFixedDelay(this::tick, 0, periodMs, TimeUnit.MILLISECONDS);
    }

    @PreDestroy
    void stop() {
        WebSocket ws = socket;
        if (ws != null) {
            // Best effort - the JVM is going down either way.
            ws.sendClose(WebSocket.NORMAL_CLOSURE, "shutting down");
        }
        scheduler.shutdownNow();
    }

    private void tick() {
        try {
            if (socket == null) {
                connect();
            } else if (registered) {
                sendHeartbeat();
            }
        } catch (Exception e) {
            log.warn("watch: tick failed ({}) - will retry", e.toString());
            dropConnection();
        }
    }

    // -- Connect + register --

    private void connect() {
        try {
            WebSocket ws = http.newWebSocketBuilder()
                    .connectTimeout(Duration.ofSeconds(10))
                    .buildAsync(URI.create(props.getHubUrl()), new HubListener())
                    .get(15, TimeUnit.SECONDS);
            socket = ws;
            registered = false;
            sendRegister(ws);
        } catch (Exception e) {
            // Hub down or unreachable: the next tick retries.
            log.warn("watch: cannot reach hub at {} ({})", props.getHubUrl(), rootCause(e));
            dropConnection();
        }
    }

    private void sendRegister(WebSocket ws) {
        RegisterRequest request = new RegisterRequest(
                agentId.toString(),
                hostname(),
                System.getProperty("os.name") + " " + System.getProperty("os.version"),
                props.getAgentVersion(),
                appVersion,
                blankToNull(props.getEnrollToken()),
                identity.credential(),
                Map.of("source", "embedded-java-agent",
                        "runtime", "jvm-" + System.getProperty("java.version")),
                hostInfo());
        send(ws, "register", request);
    }

    private void handleRegisterAck(JsonNode payload) {
        RegisterAck ack = mapper.convertValue(payload, RegisterAck.class);
        if (!ack.success()) {
            // The hub closes the socket right after this, so just log the reason.
            log.error("watch: hub rejected registration - {}", ack.message());
            return;
        }
        if (ack.config() != null) {
            identity.credential(ack.config().get("assigned_credential"));
        }
        registered = true;
        log.info("watch: registered with hub as agent {} ({})", ack.assignedId(), ack.message());
        // Do not wait a full interval for the app to show up on the dashboard.
        scheduler.execute(this::sendHeartbeat);
    }

    // -- Heartbeat --

    private void sendHeartbeat() {
        WebSocket ws = socket;
        if (ws == null || !registered) return;

        Instant now = Instant.now();
        MainAppInfo self = new MainAppInfo(
                props.getAppId(),
                props.getAppName(),
                appVersion,
                "alive",
                now,
                null,
                (int) ProcessHandle.current().pid(),
                sampleMetrics(now),
                null,
                System.getProperty("user.dir"),
                System.getProperty("user.name"),
                System.getProperty("user.name"),
                props.getLogPaths().isEmpty() ? null : props.getLogPaths());

        Heartbeat heartbeat = new Heartbeat(
                agentId.toString(),
                props.getAgentVersion(),
                appVersion,
                ManagementFactory.getRuntimeMXBean().getUptime() / 1000,
                true,
                "alive",
                now,
                List.of(self),
                Map.of("mainapp_count", 1));

        send(ws, "heartbeat", heartbeat);
    }

    /**
     * RSS is approximated by heap + non-heap usage - the JVM exposes no true
     * resident-set figure without native calls, and this tracks the same curve.
     */
    private AppMetrics sampleMetrics(Instant now) {
        var memory = ManagementFactory.getMemoryMXBean();
        long used = memory.getHeapMemoryUsage().getUsed() + memory.getNonHeapMemoryUsage().getUsed();
        return new AppMetrics(
                used,
                processCpuPercent(),
                ManagementFactory.getThreadMXBean().getThreadCount(),
                now);
    }

    /**
     * CPU percent over the window since the previous sample, derived from
     * cumulative process CPU time. Null on the first call (no window yet) or on
     * a JVM without the com.sun management extension.
     */
    private Double processCpuPercent() {
        var os = ManagementFactory.getOperatingSystemMXBean();
        if (!(os instanceof com.sun.management.OperatingSystemMXBean sun)) return null;

        long cpuNanos = sun.getProcessCpuTime();
        long nowNanos = System.nanoTime();
        if (cpuNanos < 0) return null;

        Double percent = null;
        if (lastCpuSampleAt > 0) {
            long elapsed = nowNanos - lastCpuSampleAt;
            if (elapsed > 0) {
                double share = (double) (cpuNanos - lastCpuTimeNanos) / elapsed;
                percent = Math.max(0d, share * 100d / Runtime.getRuntime().availableProcessors());
            }
        }
        lastCpuSampleAt = nowNanos;
        lastCpuTimeNanos = cpuNanos;
        return percent;
    }

    // -- Wire helpers --

    private void send(WebSocket ws, String type, Object payload) {
        try {
            String json = mapper.writeValueAsString(new Envelope<>(type, Instant.now(), payload));
            // Only the scheduler thread sends, so there is never a second
            // in-flight sendText - which the JDK client forbids.
            ws.sendText(json, true).get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.warn("watch: sending {} failed ({}) - dropping connection", type, rootCause(e));
            dropConnection();
        }
    }

    private void dropConnection() {
        WebSocket ws = socket;
        socket = null;
        registered = false;
        if (ws != null) {
            ws.abort();
        }
    }

    private final class HubListener implements WebSocket.Listener {

        /** Text frames can arrive fragmented; buffer until {@code last}. */
        private final StringBuilder buffer = new StringBuilder();

        @Override
        public void onOpen(WebSocket webSocket) {
            webSocket.request(1);
        }

        @Override
        public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
            buffer.append(data);
            if (last) {
                String message = buffer.toString();
                buffer.setLength(0);
                // Hand off so a slow handler never stalls the WS read loop.
                scheduler.execute(() -> dispatch(message));
            }
            webSocket.request(1);
            return null;
        }

        @Override
        public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
            log.warn("watch: hub closed the connection ({} {}) - will reconnect", statusCode, reason);
            dropConnection();
            return null;
        }

        @Override
        public void onError(WebSocket webSocket, Throwable error) {
            log.warn("watch: websocket error ({}) - will reconnect", error.toString());
            dropConnection();
        }
    }

    private void dispatch(String message) {
        try {
            JsonNode envelope = mapper.readTree(message);
            String type = envelope.path("type").asString("");
            JsonNode payload = envelope.path("payload");
            switch (type) {
                case "register_ack" -> handleRegisterAck(payload);
                // Patch/config/log commands need a real supervisor process to
                // act on; acknowledging them here would make the hub believe
                // work happened. Surface them instead.
                case "command" -> log.info("watch: hub sent command {} - not supported by the embedded agent",
                        payload.path("action").asString("?"));
                default -> log.debug("watch: ignoring message type {}", type);
            }
        } catch (Exception e) {
            log.warn("watch: cannot parse hub message ({})", e.toString());
        }
    }

    // -- Host facts --

    private HostInfo hostInfo() {
        var os = ManagementFactory.getOperatingSystemMXBean();
        Long memTotal = os instanceof com.sun.management.OperatingSystemMXBean sun
                ? sun.getTotalMemorySize() : null;

        Long diskTotal = null;
        Long diskFree = null;
        Path root = Path.of(System.getProperty("user.dir"));
        try {
            FileStore store = Files.getFileStore(root);
            diskTotal = store.getTotalSpace();
            diskFree = store.getUsableSpace();
        } catch (Exception ignored) {
            // Disk figures are cosmetic on the fleet page; skip them silently.
        }

        List<String> ips = ipAddresses();
        return new HostInfo(
                System.getProperty("user.name"),
                System.getenv("USERDOMAIN"),
                System.getProperty("os.name"),
                System.getProperty("os.version"),
                System.getProperty("os.arch"),
                System.getenv().getOrDefault("PROCESSOR_IDENTIFIER", System.getProperty("os.arch")),
                os.getAvailableProcessors(),
                Runtime.getRuntime().availableProcessors(),
                memTotal,
                diskTotal,
                diskFree,
                root.getRoot() == null ? root.toString() : root.getRoot().toString(),
                ips.isEmpty() ? null : ips.get(0),
                ips.isEmpty() ? null : ips,
                blankToNull(props.getLocation()),
                blankToNull(props.getAssignedTo()));
    }

    private static List<String> ipAddresses() {
        List<String> found = new ArrayList<>();
        try {
            for (NetworkInterface nic : Collections.list(NetworkInterface.getNetworkInterfaces())) {
                if (!nic.isUp() || nic.isLoopback()) continue;
                for (InetAddress address : Collections.list(nic.getInetAddresses())) {
                    if (address.isLoopbackAddress() || address.getHostAddress().contains(":")) continue;
                    found.add(address.getHostAddress());
                }
            }
        } catch (Exception ignored) {
            // Best effort - an empty list just means no IP column on the UI.
        }
        return found;
    }

    private static String hostname() {
        String fromEnv = System.getenv("COMPUTERNAME");
        if (fromEnv != null && !fromEnv.isBlank()) return fromEnv;
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            return "unknown-host";
        }
    }

    private static String blankToNull(String value) {
        return value == null || value.isBlank() ? null : value;
    }

    private static String rootCause(Throwable t) {
        Throwable cause = t;
        while (cause.getCause() != null) cause = cause.getCause();
        return cause.toString();
    }
}
