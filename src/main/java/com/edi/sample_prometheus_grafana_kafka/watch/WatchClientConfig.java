package com.edi.sample_prometheus_grafana_kafka.watch;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/** Wires the watch.client binding; the client bean itself is conditional. */
@Configuration
@EnableConfigurationProperties(WatchClientProperties.class)
class WatchClientConfig {
}
