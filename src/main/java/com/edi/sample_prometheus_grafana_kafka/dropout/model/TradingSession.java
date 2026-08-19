package com.edi.sample_prometheus_grafana_kafka.dropout.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * messageId 15 - trading session / state change for an order book (e.g. {@code EndofDay},
 * pre-opening, continuous).
 */
public record TradingSession(
        long offset,
        int partitionId,
        @JsonProperty("subset_seqnum") long subsetSeqnum,
        @JsonProperty("tcp_seqnum") long tcpSeqnum,
        int messageGroup,
        long timestamp,

        long orderBookId,
        long id,
        String name,
        int level,
        int type,
        String matchingType
) implements OrderBookScoped, Named {

    @Override
    public MessageType messageType() {
        return MessageType.TRADING_SESSION;
    }

    @Override
    public long timestampNanos() {
        return timestamp;
    }
}
