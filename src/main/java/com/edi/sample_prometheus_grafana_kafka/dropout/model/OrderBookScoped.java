package com.edi.sample_prometheus_grafana_kafka.dropout.model;

/**
 * A message that belongs to exactly one order book. Implemented by every type carrying an
 * {@code orderBookId} field, which lets a filter narrow by book without knowing the concrete type.
 */
public interface OrderBookScoped extends MarketMessage {

    long orderBookId();
}
