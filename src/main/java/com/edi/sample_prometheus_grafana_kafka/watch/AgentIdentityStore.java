package com.edi.sample_prometheus_grafana_kafka.watch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.UUID;

/**
 * Keeps the agent's identity across restarts.
 *
 * The hub keys its {@code agents} table on the UUID we send, so a fresh UUID on
 * every boot would litter the fleet view with duplicate workstations. And once
 * an enrollment token has been consumed the hub issues a credential that must be
 * echoed on every later reconnect — lose it and the agent is locked out until
 * an admin resets it. Both live in one small properties file.
 */
class AgentIdentityStore {

    private static final Logger log = LoggerFactory.getLogger(AgentIdentityStore.class);

    private static final String KEY_AGENT_ID = "agent.id";
    private static final String KEY_CREDENTIAL = "agent.credential";

    private final Path file;
    private final Properties props = new Properties();

    AgentIdentityStore(String path) {
        this.file = Path.of(path).toAbsolutePath();
        load();
    }

    private void load() {
        if (!Files.isRegularFile(file)) return;
        try (InputStream in = Files.newInputStream(file)) {
            props.load(in);
        } catch (IOException e) {
            log.warn("watch: cannot read agent identity at {} ({}) — a new identity will be minted",
                    file, e.toString());
        }
    }

    /** Stable UUID for this install; minted and persisted on first call. */
    UUID agentId() {
        String stored = props.getProperty(KEY_AGENT_ID);
        if (stored != null && !stored.isBlank()) {
            try {
                return UUID.fromString(stored.trim());
            } catch (IllegalArgumentException e) {
                log.warn("watch: stored agent id {} is not a UUID — minting a new one", stored);
            }
        }
        UUID minted = UUID.randomUUID();
        props.setProperty(KEY_AGENT_ID, minted.toString());
        save();
        log.info("watch: minted agent id {} (persisted to {})", minted, file);
        return minted;
    }

    /** Null until the hub issues one in response to an enrollment token. */
    String credential() {
        String stored = props.getProperty(KEY_CREDENTIAL);
        return stored == null || stored.isBlank() ? null : stored;
    }

    void credential(String credential) {
        if (credential == null || credential.isBlank()) return;
        if (credential.equals(props.getProperty(KEY_CREDENTIAL))) return;
        props.setProperty(KEY_CREDENTIAL, credential);
        save();
        log.info("watch: stored hub-issued credential in {}", file);
    }

    private void save() {
        try {
            Path parent = file.getParent();
            if (parent != null) Files.createDirectories(parent);
            try (OutputStream out = Files.newOutputStream(file)) {
                props.store(out, "Nayaga Watch agent identity - do not edit or share");
            }
        } catch (IOException e) {
            // Non-fatal: the agent still works this run, it just re-enrolls next boot.
            log.warn("watch: cannot persist agent identity to {} ({})", file, e.toString());
        }
    }
}
