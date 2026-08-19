package com.edi.sample_prometheus_grafana_kafka.dropout.model;

/**
 * A directory entry with a stable id and a symbolic name, e.g. an asset ({@code BBCA}), an order
 * book ({@code BBCA_RG}), a participant or an actor.
 */
public interface Named extends MarketMessage {

    long id();

    String name();
}
